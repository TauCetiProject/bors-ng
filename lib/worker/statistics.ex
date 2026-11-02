defmodule BorsNG.Worker.Statistics do
  @moduledoc "Read-only, bounded-window reporting from retained batch and candidate facts."
  import Ecto.Query
  alias BorsNG.Database.{Batch, Candidate, CandidateEvent, Repo}
  @windows [{"1h", 3600}, {"3h", 10800}, {"24h", 86400}, {"7d", 604_800}]

  def observation(project, base, now \\ System.system_time(:second)) do
    since = DateTime.from_unix!(now - 604_800) |> DateTime.to_naive()

    query =
      from(b in Batch,
        where:
          b.project_id == ^project.id and b.state in [:ok, :error, :canceled] and
            b.updated_at >= ^since
      )

    query = if base, do: from(b in query, where: b.into_branch == ^base), else: query
    batches = Repo.all(query, timeout: 5000)
    ids = Enum.map(batches, & &1.id)
    candidates = Repo.all(from(c in Candidate, where: c.batch_id in ^ids), timeout: 5000)
    candidate_ids = Enum.map(candidates, & &1.id)

    events =
      Repo.all(
        from(e in CandidateEvent,
          where:
            e.candidate_id in ^candidate_ids and
              e.kind in ["constructed", "dispatched", "promoted"],
          order_by: e.id
        ),
        timeout: 5000
      )

    %{
      schema: "tauceti-bors.statistics/v1",
      repo: project.name,
      base: base,
      observed_at: now,
      queue_authority: false,
      definitions: %{
        completed:
          "terminal batches by their final database update; canceled batches are separate",
        attempts: "distinct dispatched batch/PR pairs among completed batches in each window",
        landed:
          "distinct PRs with a matching constructed commit and a Bors main-promotion receipt",
        coverage: "retained candidate journal only; absent membership is unknown, not zero"
      },
      windows: summarize(batches, candidates, events, now)
    }
  end

  def summarize(batches, candidates, events, now) do
    by_candidate = Enum.group_by(events, & &1.candidate_id)
    by_batch = Enum.group_by(candidates, & &1.batch_id)

    staged =
      Enum.filter(candidates, fn c ->
        Enum.any?(by_candidate[c.id] || [], &(&1.kind == "dispatched"))
      end)

    appearances = for c <- staged, member <- members(c), do: {c.batch_id, member["pr"]}

    landings =
      for c <- candidates,
          e <- by_candidate[c.id] || [],
          e.kind == "promoted",
          e.source in ["github.base_push_response", "github.base_recovery_read"],
          e.details["base"] == c.into_branch,
          e.details["base_sha"] == c.snapshot["base_sha"],
          Enum.any?(by_candidate[c.id] || [], fn constructed ->
            constructed.kind == "constructed" and
              constructed.details["commit"] == e.details["commit"] and
              constructed.details["base_sha"] == e.details["base_sha"]
          end),
          member <- members(c),
          do: {div(e.observed_at, 1_000_000), member["pr"]}

    Enum.map(@windows, fn {label, seconds} ->
      selected =
        Enum.filter(batches, &(completed_at(&1) > now - seconds and completed_at(&1) <= now))

      ids = MapSet.new(selected, & &1.id)

      pairs =
        appearances |> Enum.filter(fn {id, _} -> MapSet.member?(ids, id) end) |> MapSet.new()

      prs = MapSet.new(pairs, fn {_, pr} -> pr end)

      landed =
        landings
        |> Enum.filter(fn {at, _} -> at > now - seconds and at <= now end)
        |> MapSet.new(fn {_, pr} -> pr end)

      states = Enum.frequencies_by(selected, & &1.state)

      %{
        window: label,
        seconds: seconds,
        completed_batches: length(selected),
        successful_batches: Map.get(states, :ok, 0),
        failed_batches: Map.get(states, :error, 0),
        canceled_batches: Map.get(states, :canceled, 0),
        landed_prs: MapSet.size(landed),
        staging_pr_runs: MapSet.size(pairs),
        staging_unique_prs: MapSet.size(prs),
        mean_staging_runs_per_pr:
          if(MapSet.size(prs) > 0, do: MapSet.size(pairs) / MapSet.size(prs)),
        batches_with_unknown_membership:
          Enum.count(selected, &(not Map.has_key?(by_batch, &1.id)))
      }
    end)
  end

  defp members(candidate) do
    (candidate.snapshot["members"] || [])
    |> Enum.filter(&(is_integer(&1["pr"]) and &1["pr"] > 0))
  end

  defp completed_at(%{updated_at: %NaiveDateTime{} = at}),
    do: at |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix()

  defp completed_at(%{updated_at: %DateTime{} = at}), do: DateTime.to_unix(at)
end
