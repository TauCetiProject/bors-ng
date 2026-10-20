defmodule BorsNG.Worker.CandidateCancellation do
  @moduledoc "Bounded exact-candidate cancellation through a trusted default-branch workflow."
  import Ecto.Query
  alias BorsNG.Database.{Batch, Candidate, CandidateEvent, OrchestrationJob, Project, Repo}
  alias BorsNG.Worker.CandidateJournal, as: Journal
  alias BorsNG.Worker.Orchestration.Outbox
  require Logger

  @protected ["promotion_requested", "promoted", "main_pushed", "post_merge"]

  # Called in the same transaction as the explicit withdrawal. No HTTP here.
  def enqueue(batch) do
    project = Repo.get!(Project, batch.project_id)
    candidate = Journal.current(batch)

    if Outbox.enabled?() and project.name == BorsNG.ProjectConfig.repo() and
         project.repo_xref == BorsNG.ProjectConfig.repo_id() and
         System.get_env("BORS_STAGE_DISPATCH_PROJECT") == BorsNG.ProjectConfig.repo() and
         candidate do
      requested =
        Repo.all(
          from(e in CandidateEvent,
            where: e.candidate_id == ^candidate.id and e.kind == "dispatch_requested",
            limit: 2
          )
        )

      case requested do
        [event] ->
          payload = %{
            "repository" => BorsNG.ProjectConfig.repo(),
            "repository_id" => BorsNG.ProjectConfig.repo_id(),
            "batch_id" => batch.id,
            "candidate_id" => candidate.id,
            "generation" => candidate.generation,
            "head_sha" => batch.commit,
            "base_sha" => batch.base_sha,
            "branch" => batch.into_branch,
            "dispatch_at" => event.observed_at,
            "expires_at" => Outbox.now() + 1800
          }

          Outbox.enqueue(project.id, "cancel_run", payload, "cancel-run:#{candidate.id}")

        _ ->
          :ok
      end
    end
  end

  # Public read-only authority; knowing an ID never creates or extends authority.
  def authority(id) do
    with %OrchestrationJob{kind: "cancel_run", state: state} = job <-
           Repo.get(OrchestrationJob, id),
         true <- state in ["pending", "running"] and job.attempts <= 6,
         p when is_map(p) <- Outbox.payload(job),
         true <- valid_payload?(p),
         true <- p["expires_at"] > Outbox.now(),
         %Project{} = project <- Repo.get(Project, job.project_id),
         true <-
           project.name == BorsNG.ProjectConfig.repo() and
             project.repo_xref == BorsNG.ProjectConfig.repo_id(),
         %Batch{state: :canceled} = batch <- Repo.get(Batch, p["batch_id"]),
         true <-
           batch.project_id == job.project_id and batch.commit == p["head_sha"] and
             batch.base_sha == p["base_sha"] and batch.into_branch == p["branch"],
         %Candidate{} = candidate <- Journal.current(batch),
         true <- candidate.id == p["candidate_id"] and candidate.generation == p["generation"],
         false <-
           Repo.exists?(
             from(e in CandidateEvent,
               where: e.candidate_id == ^candidate.id and e.kind in ^@protected
             )
           ),
         true <- unique_binding?(candidate, p) do
      bound =
        Repo.all(
          from(e in CandidateEvent,
            where: e.candidate_id == ^candidate.id and e.kind == "cancellation_run_bound",
            limit: 2
          )
        )

      case bound do
        [] ->
          {:ok, Map.put(p, "intent_id", job.id)}

        [%{details: %{"run_id" => run_id, "run_attempt" => 1} = details}]
        when is_integer(run_id) and run_id > 0 and map_size(details) == 2 ->
          {:ok, p |> Map.put("intent_id", job.id) |> Map.merge(details)}

        _ ->
          {:error, :unauthorized_or_expired}
      end
    else
      _ -> {:error, :unauthorized_or_expired}
    end
  end

  defp valid_payload?(p) do
    p["repository"] == BorsNG.ProjectConfig.repo() and
      p["repository_id"] == BorsNG.ProjectConfig.repo_id() and
      Enum.all?(
        ["batch_id", "candidate_id", "generation", "dispatch_at", "expires_at"],
        &(is_integer(p[&1]) and p[&1] > 0)
      ) and
      Enum.all?(
        ["head_sha", "base_sha"],
        &(is_binary(p[&1]) and Regex.match?(~r/^[0-9a-f]{40}$/, p[&1]))
      ) and
      is_binary(p["branch"]) and p["branch"] != "" and
      not String.contains?(p["branch"], [" ", "\n", "\r"])
  end

  # repository_dispatch exposes the bound title, not its client_payload. The
  # journal must map that title to exactly one generation. Same-SHA rebuilds
  # with indistinguishable dispatch identities fail closed, even if only one run
  # is currently visible. Read at most 101 rows, never scan historical API pages.
  defp unique_binding?(candidate, p) do
    constructed =
      Repo.all(
        from(e in CandidateEvent,
          join: c in Candidate,
          on: c.id == e.candidate_id,
          where: c.batch_id == ^candidate.batch_id and e.kind == "constructed",
          select: {c.id, e.details},
          limit: 101
        )
      )

    requests =
      Repo.all(
        from(e in CandidateEvent,
          where: e.candidate_id == ^candidate.id and e.kind == "dispatch_requested",
          limit: 2
        )
      )

    matches =
      Enum.filter(constructed, fn {_, d} ->
        d["commit"] == p["head_sha"] and d["base_sha"] == p["base_sha"]
      end)

    length(constructed) <= 100 and matches != [] and
      Enum.map(matches, &elem(&1, 0)) == [candidate.id] and
      Enum.map(requests, &{&1.observed_at, &1.details["commit"], &1.details["base_sha"]}) ==
        [{p["dispatch_at"], p["head_sha"], p["base_sha"]}]
  end

  def title(p),
    do:
      "bors branch=#{p["branch"]} batch=#{p["batch_id"]} head=#{p["head_sha"]} base=#{p["base_sha"]}"

  def validate_run(run, p) when is_map(run) do
    with true <-
           run["event"] == "repository_dispatch" and
             run["path"] == ".github/workflows/pr-build.yml" and
             run["workflow_id"] == BorsNG.ProjectConfig.cancellation_workflow_id() and
             run["display_title"] == title(p),
         true <-
           get_in(run, ["repository", "id"]) == BorsNG.ProjectConfig.repo_id() and
             get_in(run, ["repository", "full_name"]) == BorsNG.ProjectConfig.repo(),
         true <-
           Enum.all?(["actor", "triggering_actor"], fn k ->
             get_in(run, [k, "id"]) == BorsNG.ProjectConfig.actor_id() and
               get_in(run, [k, "login"]) == "tau-ceti-bors[bot]" and
               get_in(run, [k, "type"]) == "Bot"
           end),
         true <- is_integer(run["id"]) and run["id"] > 0 and run["run_attempt"] == 1,
         {:ok, created, _} <- DateTime.from_iso8601(run["created_at"] || ""),
         true <-
           DateTime.to_unix(created, :microsecond) >= p["dispatch_at"] - 60_000_000 and
             DateTime.to_unix(created) <= p["expires_at"] do
      case run["status"] do
        "completed" -> {:terminal, run["conclusion"]}
        s when s in ["queued", "in_progress", "waiting", "requested", "pending"] -> {:ok, run}
        _ -> {:error, :invalid_run_state}
      end
    else
      _ -> {:error, :invalid_run_binding}
    end
  end

  def validate_run(_, _), do: {:error, :invalid_run_binding}

  def deliver(job) do
    with {:ok, p} <- authority(job.id),
         true <- Outbox.owned?(job),
         conn <- Project.installation_project_connection(job.project_id, Repo),
         {:ok, run} <- BorsNG.GitHub.cancellation_call(:cancellation_lookup, conn, {p}),
         {:ok, run} <-
           if(is_nil(run), do: {:retry, :run_not_discovered}, else: validate_run(run, p)),
         :ok <- bind_run(p, run),
         :ok <- deliver_once(job, conn, p, run) do
      # A 204 acknowledges dispatch, not cancellation. Reconcile on durable retry.
      throw({:cancellation_retry, :awaiting_ci_terminal})
    else
      {:terminal, reason} ->
        outcome(job, {:completed, reason})

      {:error, :unauthorized_or_expired} ->
        outcome(job, :unauthorized_or_expired)

      {:error, reason}
      when reason in [
             :invalid_run_binding,
             :invalid_run_state,
             :run_binding_changed,
             :duplicate_staging_runs,
             :dispatch_lookup_invalid,
             :dispatch_lookup_truncated
           ] ->
        outcome(job, reason)

      {:error, _, code} when code in [401, 403, 404] ->
        outcome(job, {:http, code})

      false ->
        outcome(job, :lease_lost)

      error ->
        throw({:cancellation_retry, error})
    end
  end

  defp deliver_once(job, conn, p, run) do
    accepted =
      Repo.exists?(
        from(e in CandidateEvent,
          where: e.candidate_id == ^p["candidate_id"] and e.kind == "cancellation_requested"
        )
      )

    attempts =
      Repo.aggregate(
        from(e in CandidateEvent,
          where:
            e.candidate_id == ^p["candidate_id"] and e.kind == "cancellation_delivery_attempted"
        ),
        :count
      )

    cond do
      accepted ->
        :ok

      attempts >= 2 ->
        throw({:cancellation_retry, :ambiguous_delivery_limit})

      true ->
        with :ok <- BorsNG.GitHub.cancellation_call(:cancellation_capability, conn, {}),
             {:ok, _} <- authority(job.id) do
          # A lost response can have accepted the dispatch. Permit at most one
          # ambiguity replay; known204 delivery is never dispatched again.
          record(p, "delivery_attempted", %{run_id: run["id"], run_attempt: 1})

          case BorsNG.GitHub.cancellation_call(
                 :cancellation_dispatch,
                 conn,
                 {job.id, run["id"], run["run_attempt"]}
               ) do
            :ok -> record(p, "requested", %{run_id: run["id"], run_attempt: 1})
            error -> error
          end
        end
    end
  end

  defp bind_run(p, run) do
    events =
      Repo.all(
        from(e in CandidateEvent,
          where: e.candidate_id == ^p["candidate_id"] and e.kind == "cancellation_run_bound",
          limit: 2
        )
      )

    binding = %{"run_id" => run["id"], "run_attempt" => run["run_attempt"]}

    case events do
      [] ->
        Journal.event(
          Repo.get!(Candidate, p["candidate_id"]),
          "cancellation_run_bound",
          "github.actions_read",
          binding
        )

      [%{details: ^binding}] ->
        :ok

      _ ->
        {:error, :run_binding_changed}
    end
  end

  defp outcome(job, reason) do
    Logger.info("candidate_cancellation_terminal intent=#{job.id} outcome=#{inspect(reason)}")
    record(Outbox.payload(job), "terminal", %{outcome: inspect(reason)})
    :ok
  end

  defp record(p, phase, details) do
    Journal.event(
      Repo.get!(Candidate, p["candidate_id"]),
      "cancellation_#{phase}",
      "bors.cancellation",
      details
    )
  end
end
