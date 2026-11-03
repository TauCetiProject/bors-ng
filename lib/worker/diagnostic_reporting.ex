defmodule BorsNG.Worker.DiagnosticReporting do
  @moduledoc "Durable diagnostic incidents; alert delivery never owns the staging lane."
  import Ecto.Query
  require Logger
  alias BorsNG.Database.{Candidate, CandidateEvent, OrchestrationJob, Project, Repo}
  alias BorsNG.Worker.{CandidateJournal, Zulip}
  alias BorsNG.Worker.Orchestration.Outbox

  def record(candidate, result, located?) do
    reason =
      if result["complete"] != true,
        do: result["reason"] || "collector_failure",
        else: "unlocated_failure"

    reason = if Regex.match?(~r/\A[a-z_]{1,80}\z/, reason), do: reason, else: "collector_failure"
    root = root(candidate)

    previous =
      Repo.all(
        from(e in CandidateEvent,
          join: c in Candidate,
          on: c.id == e.candidate_id,
          where:
            c.project_id == ^candidate.project_id and e.kind == "diagnostic_degraded" and
              e.details["root"] == ^root
        )
      )

    # Successful collection closes transport/parser incidents even when no
    # changed file owns the failure. That separate state remains visible.
    recovered =
      if result["complete"] == true,
        do: Enum.uniq(Enum.map(previous, & &1.details["incident"])),
        else: []

    incidents =
      Enum.map(recovered, &{&1, "diagnostic_recovered"}) ++
        if(located?, do: [], else: [{"#{root}:#{reason}", "diagnostic_degraded"}])

    for {incident, kind} <- incidents do
      latest =
        Repo.one(
          from(e in CandidateEvent,
            where:
              (e.candidate_id == ^candidate.id or ^kind == "diagnostic_recovered") and
                e.details["incident"] == ^incident and
                e.kind in ["diagnostic_degraded", "diagnostic_recovered"],
            order_by: [desc: e.id],
            limit: 1
          )
        )

      unless latest && latest.kind == kind do
        CandidateJournal.event(candidate, kind, "bors.diagnostics", %{
          root: root,
          incident: incident,
          reason: List.last(String.split(incident, ":", parts: 2)),
          parser_version: result["parser_version"]
        })

        if Outbox.enabled?() do
          delivered =
            Repo.one(
              from(e in CandidateEvent,
                where:
                  e.kind == "diagnostic_alert_delivered" and
                    e.details["incident"] == ^incident,
                order_by: [desc: e.id],
                limit: 1
              )
            )

          at =
            if delivered && kind == "diagnostic_degraded",
              do: max(Outbox.now(), div(delivered.observed_at, 1_000_000) + 900),
              else: Outbox.now()

          Outbox.enqueue(
            candidate.project_id,
            "diagnostic_alert",
            incident,
            "diagnostic-alert:#{candidate.project_id}:#{incident}",
            at
          )
        end
      end
    end

    unless located?,
      do: Logger.warning("bors_diagnostics_degraded candidate=#{candidate.id} reason=#{reason}")

    :ok
  rescue
    error ->
      Logger.error(
        "bors_diagnostic_reporting_failed candidate=#{candidate.id} cause=#{inspect(error.__struct__)}"
      )

      :ok
  end

  defp root(candidate), do: ancestry([candidate.id], MapSet.new()) |> Enum.min()
  defp ancestry([], seen), do: MapSet.to_list(seen)

  defp ancestry([id | rest], seen) do
    if MapSet.member?(seen, id) do
      ancestry(rest, seen)
    else
      candidate = Repo.get(Candidate, id)

      parents =
        if candidate,
          do:
            Enum.map(candidate.snapshot["members"] || [], & &1["parent_candidate_id"])
            |> Enum.reject(&is_nil/1),
          else: []

      ancestry(rest ++ parents, MapSet.put(seen, id))
    end
  end

  def deliver(project_id, incident, sender \\ &Zulip.deliver/2) do
    events =
      Repo.all(
        from(e in CandidateEvent,
          join: c in Candidate,
          on: c.id == e.candidate_id,
          where: c.project_id == ^project_id and e.details["incident"] == ^incident,
          order_by: e.id,
          select: {e, c.batch_id}
        )
      )

    states =
      events
      |> Enum.filter(fn {e, _} -> e.kind in ["diagnostic_degraded", "diagnostic_recovered"] end)
      |> Enum.reduce(%{}, fn {e, batch}, acc -> Map.put(acc, e.candidate_id, {e, batch}) end)

    latest_recovery =
      events
      |> Enum.filter(fn {e, _} -> e.kind == "diagnostic_recovered" end)
      |> Enum.map(fn {e, _} -> e.id end)
      |> Enum.max(fn -> 0 end)

    unresolved =
      Enum.count(states, fn {_, {e, _}} ->
        e.kind == "diagnostic_degraded" and e.id > latest_recovery
      end)

    {_, {latest, batch}} = Enum.max_by(states, fn {_, {e, _}} -> e.id end)

    affected_batches =
      events
      |> Enum.filter(fn {e, _} -> e.kind == "diagnostic_degraded" end)
      |> Enum.map(&elem(&1, 1))
      |> Enum.uniq()
      |> length()

    observation =
      Repo.one(
        from(e in CandidateEvent,
          where: e.candidate_id == ^latest.candidate_id and e.kind == "failure_diagnostics",
          order_by: [desc: e.id],
          limit: 1
        )
      )

    run = if observation, do: get_in(observation.details, ["binding", "run_id"])
    project = Repo.get!(Project, project_id)

    run_link =
      if is_integer(run),
        do: " [Actions run](https://github.com/#{project.name}/actions/runs/#{run}).",
        else: ""

    title =
      if unresolved > 0,
        do: "Bors diagnostic degradation",
        else: "Bors diagnostic collection recovered"

    message =
      "#{title} for #{project.name}: #{unresolved} unresolved of #{map_size(states)} affected candidates across #{affected_batches} batches. " <>
        "Reason: `#{latest.details["reason"]}`. [Batch #{batch}](https://bors.taucetiproject.org/batches/#{batch}). " <>
        run_link <>
        " The queue continues with fallback isolation; no operator acknowledgement is required."

    # New messages in the incident topic notify operators; old-message edits do not.
    case sender.(message, topic: "bors diagnostics") do
      {:ok, id} ->
        CandidateJournal.event(
          Repo.get!(Candidate, latest.candidate_id),
          "diagnostic_alert_delivered",
          "bors.zulip",
          %{incident: incident, message_id: id, unresolved: unresolved}
        )

        :ok

      {:error, reason} ->
        throw({:diagnostic_retry, reason})
    end
  end

  def batch(batch_id) do
    events =
      Repo.all(
        from(e in CandidateEvent,
          join: c in Candidate,
          on: c.id == e.candidate_id,
          where:
            c.batch_id == ^batch_id and
              e.kind in [
                "failure_diagnostics",
                "failure_partition",
                "diagnostic_degraded",
                "diagnostic_recovered"
              ],
          order_by: e.id
        )
      )

    observation = events |> Enum.filter(&(&1.kind == "failure_diagnostics")) |> List.last()
    partition = events |> Enum.filter(&(&1.kind == "failure_partition")) |> List.last()

    incident =
      events
      |> Enum.filter(&(&1.kind in ["diagnostic_degraded", "diagnostic_recovered"]))
      |> List.last()

    %{
      coverage: if(observation, do: observation.details["result"], else: nil),
      degraded_reason:
        if(incident && incident.kind == "diagnostic_degraded",
          do: incident.details["reason"],
          else: nil
        ),
      isolation: if(partition, do: partition.details["strategy"], else: nil)
    }
  end

  def summary(project_id) do
    since = (Outbox.now() - 604_800) * 1_000_000

    events =
      Repo.all(
        from(e in CandidateEvent,
          join: c in Candidate,
          on: c.id == e.candidate_id,
          where:
            c.project_id == ^project_id and e.observed_at >= ^since and
              e.kind in ["diagnostic_degraded", "diagnostic_recovered", "failure_partition"],
          order_by: e.id
        )
      )

    states =
      events
      |> Enum.filter(&(&1.kind != "failure_partition"))
      |> Enum.reduce(%{}, &Map.put(&2, &1.details["incident"], &1))

    candidate_ids = Enum.map(events, & &1.candidate_id) |> Enum.uniq()

    batches =
      Map.new(
        Repo.all(from(c in Candidate, where: c.id in ^candidate_ids, select: {c.id, c.batch_id}))
      )

    %{
      window: "7d",
      coverage: "retained incident journal; unrecorded historical incidents remain unknown",
      affected_candidates:
        Enum.count(
          events
          |> Enum.filter(&(&1.kind == "diagnostic_degraded"))
          |> Enum.uniq_by(& &1.candidate_id)
        ),
      unresolved_incidents: Enum.count(states, fn {_, e} -> e.kind == "diagnostic_degraded" end),
      unresolved_candidates:
        states
        |> Map.values()
        |> Enum.filter(&(&1.kind == "diagnostic_degraded"))
        |> Enum.uniq_by(& &1.candidate_id)
        |> length(),
      affected_batches:
        events
        |> Enum.filter(&(&1.kind == "diagnostic_degraded"))
        |> Enum.map(fn e -> batches[e.candidate_id] end)
        |> Enum.uniq()
        |> length(),
      blind_retries:
        Enum.count(
          events,
          &(&1.kind == "failure_partition" and
              &1.details["strategy"] in [
                "deterministic_bisection",
                "balanced_dependency_components"
              ])
        ),
      pending_alerts:
        Repo.aggregate(
          from(j in OrchestrationJob,
            where:
              j.project_id == ^project_id and
                j.kind == "diagnostic_alert" and j.state in ["pending", "running"]
          ),
          :count
        )
    }
  end
end
