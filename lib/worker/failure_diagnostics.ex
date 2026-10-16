defmodule BorsNG.Worker.FailureDiagnostics do
  @moduledoc "Candidate-bound, durable observations of untrusted terminal compiler output."
  import Ecto.Query
  alias BorsNG.Database.{CandidateEvent, Repo}
  alias BorsNG.Worker.{CandidateAssembler, CandidateJournal, FailureIsolation}

  @steps [
    "Preflight exact-candidate import headers",
    "Build exact candidate under bwrap with workflow-pinned audits, offline"
  ]

  # Runs in the existing per-project orchestration executor, outside the shared
  # GitHub server/scheduler. A persisted unavailable result is final for this
  # immutable candidate/run attempt, including after process restart.
  def observe(batch, evidence, conn, collector \\ &collect/2) do
    candidate = CandidateJournal.current(batch)
    hints = FailureIsolation.hints(candidate)

    if FailureIsolation.enabled?() and not is_nil(candidate) and is_map(hints) and
         FailureIsolation.ownership_complete?(hints) and
         length(candidate.snapshot["members"]) > 1 and eligible?(batch, evidence) and
         fresh_run?(candidate, evidence) do
      binding = binding(candidate, batch.commit, evidence)

      unless observation(candidate, binding) do
        jobs = job_ids(evidence)

        result =
          if length(jobs) in 1..2,
            do: collector.(conn, jobs),
            else: %{"complete" => false, "reason" => "ambiguous_failed_jobs"}

        CandidateJournal.event(candidate, "failure_diagnostics", "bors.terminal_logs", %{
          binding: binding,
          result: result,
          authority: "retry_only",
          proof_authority: false
        })
      end
    end

    :ok
  end

  defp fresh_run?(candidate, evidence) do
    with value when is_binary(value) <- evidence[:run_created_at],
         {:ok, created, _} <- DateTime.from_iso8601(value) do
      DateTime.to_unix(created, :microsecond) >= candidate.observed_at
    else
      _ -> false
    end
  end

  defp eligible?(batch, evidence) do
    evidence[:diagnostic] == nil and evidence[:conclusion] == "failure" and
      evidence[:workflow] == ".github/workflows/pr-build.yml" and
      evidence[:identity] ==
        "bors branch=#{batch.into_branch} batch=#{batch.id} head=#{batch.commit} base=#{batch.base_sha}" and
      is_integer(evidence[:run_id]) and evidence[:run_id] > 0 and
      is_integer(evidence[:run_attempt]) and evidence[:run_attempt] > 0
  end

  defp job_ids(evidence) do
    (evidence[:failed_steps] || [])
    |> Enum.filter(&(&1[:job_name] == "sandboxed-build" and &1[:name] in @steps))
    |> Enum.map(& &1[:job_id])
    |> Enum.filter(&(is_integer(&1) and &1 > 0))
    |> Enum.uniq()
    |> Enum.sort()
  end

  def binding(candidate, commit, evidence) do
    CandidateAssembler.identity(candidate)
    |> Map.merge(%{
      commit: commit,
      run_id: evidence[:run_id] || evidence["run_id"],
      run_attempt: evidence[:run_attempt] || evidence["run_attempt"],
      job_ids: job_ids(normalize_evidence(evidence)),
      workflow: evidence[:workflow] || evidence["workflow"]
    })
    |> Jason.encode!()
    |> Jason.decode!()
  end

  defp normalize_evidence(evidence) do
    %{
      failed_steps:
        Enum.map(evidence[:failed_steps] || evidence["failed_steps"] || [], fn step ->
          %{
            job_id: step[:job_id] || step["job_id"],
            job_name: step[:job_name] || step["job_name"],
            name: step[:name] || step["name"]
          }
        end)
    }
  end

  defp observation(candidate, binding) do
    Repo.all(
      from(e in CandidateEvent,
        where:
          e.candidate_id == ^candidate.id and e.kind == "failure_diagnostics" and
            e.source == "bors.terminal_logs",
        order_by: [desc: e.id]
      )
    )
    |> Enum.find(&(&1.details["binding"] == binding))
  end

  # Only the latest trusted classifier observation can select an observation.
  # A changed generation/head/base/run/attempt/job cannot reuse another's logs.
  def suspects(candidate, hints) do
    case diagnosis(candidate, hints) do
      {:ok, prs, _} -> {:ok, prs}
      other -> other
    end
  end

  def diagnosis(candidate, hints) do
    with false <- is_nil(candidate),
         event when not is_nil(event) <-
           Repo.one(
             from(e in CandidateEvent,
               where:
                 e.candidate_id == ^candidate.id and e.kind == "failure_classified" and
                   e.source == "bors.trusted_failure",
               order_by: [desc: e.id],
               limit: 1
             )
           ),
         %{"classification" => "candidate_failure", "evidence" => evidence, "commit" => commit} <-
           event.details,
         true <- evidence["diagnostic"] == nil,
         true <- CandidateJournal.commit_binding(candidate)["commit"] == commit,
         observation when not is_nil(observation) <-
           observation(candidate, binding(candidate, commit, evidence)) do
      case map_paths(hints, observation.details["result"]) do
        {:ok, prs} ->
          {:ok, prs,
           %{
             "binding" => observation.details["binding"],
             "diagnostics" => observation.details["result"]
           }}

        other ->
          other
      end
    else
      _ -> :unavailable
    end
  end

  def map_paths(%{"changed_files" => changed} = hints, %{
        "complete" => true,
        "jobs" => jobs
      }) do
    observations = Enum.flat_map(jobs, & &1["observations"])

    mapped =
      Enum.map(observations, fn observation ->
        owners =
          Enum.map(observation["paths"], fn path ->
            Enum.filter(changed, fn member ->
              path in member["paths"] or
                (Map.get(hints, "dependencies_complete", hints["complete"]) == true and
                   path in (member["dependency_paths"] || []))
            end)
            |> Enum.map(& &1["pr"])
          end)

        # Both sides of an audit collision are retained. An unchanged base
        # declaration has no changed owner; every changed side is suspected.
        if observation["kind"] == "duplicate_declaration" or Enum.all?(owners, &(&1 != [])),
          do: List.flatten(owners),
          else: []
      end)

    if FailureIsolation.ownership_complete?(hints) and mapped != [] and
         Enum.all?(mapped, &(&1 != [])),
       do: {:ok, mapped |> List.flatten() |> Enum.uniq()},
       else: :unavailable
  end

  def map_paths(_, _), do: :unavailable

  defp collect(conn, jobs) do
    input =
      Path.join(
        System.tmp_dir!(),
        "bors-diagnostics-#{Base.encode16(:crypto.strong_rand_bytes(16))}.json"
      )

    try do
      {{_, _}, repo_id} = conn
      {:ok, file} = File.open(input, [:write, :exclusive])
      File.chmod!(input, 0o600)
      IO.binwrite(file, Jason.encode!(%{repository_id: repo_id, job_ids: jobs}))
      File.close(file)

      with {:ok, token} <- BorsNG.GitHub.git_token(conn),
           {output, 0} <-
             System.cmd(
               "python3",
               [Application.app_dir(:bors, "priv/failure_diagnostics.py"), input],
               env: [{"BORS_DIAGNOSTICS_TOKEN", token}],
               stderr_to_stdout: false
             ),
           {:ok, result} <- Jason.decode(output) do
        result
      else
        _ -> %{"complete" => false, "reason" => "collector_unavailable"}
      end
    rescue
      _ -> %{"complete" => false, "reason" => "collector_unavailable"}
    after
      File.rm(input)
    end
  end
end
