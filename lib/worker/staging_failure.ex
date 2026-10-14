defmodule BorsNG.Worker.StagingFailure do
  @moduledoc "Bind an inconclusive Actions result to the exact trusted staging build."

  def build_source(%{"sha" => head, "statuses" => statuses}, head, repo) when is_list(statuses) do
    latest = statuses |> Enum.filter(&is_map/1) |> Enum.uniq_by(& &1["context"])

    case Enum.find(
           latest,
           &(&1["context"] in ["build", "bump-guard", "scope"] and
               &1["state"] in ["failure", "error"])
         ) do
      %{"state" => state, "creator" => %{"id" => 41_898_282}, "target_url" => url}
      when state in ["failure", "error"] and is_binary(url) ->
        uri = URI.parse(url)
        prefix = "/#{repo}/actions/runs/"

        if uri.scheme == "https" and uri.host == "github.com" and
             is_binary(uri.path) and String.starts_with?(uri.path, prefix) and
             is_nil(uri.query) and is_nil(uri.fragment) do
          case Integer.parse(String.replace_prefix(uri.path, prefix, "")) do
            {id, ""} when id > 0 -> {:lookup, id}
            _ -> :unavailable
          end
        else
          :unavailable
        end

      nil ->
        # A newer status superseded the database failure, or the finalizer
        # has not posted it yet. This is propagation, not a failed retry.
        :pending

      _ ->
        :unavailable
    end
  end

  def build_source(_, _, _), do: :unavailable

  def classify_run(run, batch, head, branch, base) when is_map(run) do
    title = "bors branch=#{branch} batch=#{batch} head=#{head} base=#{base}"

    older =
      ~r/^bors branch=#{Regex.escape(branch)} batch=(\d+) head=#{Regex.escape(head)} base=#{Regex.escape(base || "")}$/

    if run["event"] == "repository_dispatch" and
         run["path"] == ".github/workflows/pr-build.yml" and
         run["display_title"] == title do
      case {run["status"], run["conclusion"]} do
        {"completed", "cancelled"} -> :cancelled
        {"completed", result} when result in ["success", "failure", "timed_out"] -> :conclusive
        {status, _} when status in ["queued", "pending", "in_progress", "waiting"] -> :pending
        _ -> :unavailable
      end
    else
      case Regex.run(older, run["display_title"] || "") do
        [_, previous] ->
          if run["event"] == "repository_dispatch" and
               run["path"] == ".github/workflows/pr-build.yml" and
               String.to_integer(previous) < batch,
             do: :pending,
             else: :unavailable

        _ ->
          :unavailable
      end
    end
  end

  def classify_run(_, _, _, _, _), do: :unavailable

  # GitHub's job/step conclusions are workflow-owned evidence. No candidate
  # log text, uploaded telemetry, metrics or artifact is consulted.
  @candidate_steps [
    "Preflight exact-candidate import headers",
    "Build exact candidate under bwrap with workflow-pinned audits, offline",
    "Scope guard — allow TauCeti sources and validated pins",
    "Attest exact-head Lake configuration before executing Lake",
    "Validate the Lake-pin bump before building",
    "Candidate source shape guard (reject symlinks)",
    "Import-boundary guard (TauCeti/ and TauCeti.lean ⊬ roadmap/review)",
    "Reject option overrides and heartbeat instrumentation",
    "Temporary Mathlib shims have machine-checkable expiry targets",
    "Dot-notation namespace lint (Mathlib type namespaces stay at root)"
  ]

  @infrastructure_prefixes [
    "Set up job",
    "Raise vm.",
    "Resolve PR head",
    "Checkout ",
    "Install ",
    "Restore ",
    "Fetch ",
    "Decide ",
    "Configure the Lake artifact cache",
    "Save verified Lake archive",
    "Prepare trusted read-only Lean watchdog",
    "Build the trusted environment-lint driver",
    "Hand the merge-group oleans",
    "Summarise CI telemetry",
    "Upload CI telemetry"
  ]

  def run_evidence(run) do
    %{
      run_id: run["id"],
      run_attempt: run["run_attempt"],
      run_url: run["html_url"],
      conclusion: run["conclusion"],
      workflow: run["path"],
      identity: run["display_title"]
    }
  end

  def classify_jobs(run, %{"total_count" => total, "jobs" => jobs})
      when is_integer(total) and is_list(jobs) and total == length(jobs) and total > 0 and
             total <= 100 do
    valid = Enum.all?(jobs, &(&1["run_id"] == run["id"] and &1["status"] == "completed"))

    interrupted =
      run["conclusion"] == "cancelled" or
        Enum.any?(
          jobs,
          &(&1["conclusion"] in ["cancelled", "timed_out", "stale", "action_required"])
        )

    failed =
      Enum.flat_map(jobs, fn j ->
        pin_validation_ran =
          Enum.any?(j["steps"] || [], fn s ->
            s["name"] == "Validate the Lake-pin bump before building" and
              s["conclusion"] not in [nil, "skipped"]
          end)

        Enum.filter(j["steps"] || [], &(&1["conclusion"] == "failure"))
        |> Enum.map(
          &%{
            job_id: j["id"],
            job_name: j["name"],
            name: &1["name"],
            number: &1["number"],
            pin_validation_ran: pin_validation_ran
          }
        )
      end)

    evidence = Map.put(run_evidence(run), :failed_steps, failed)

    # Report/finalizer steps deliberately exit nonzero for a red verdict too.
    # They cannot establish that the cause was infrastructure. Prefer primary
    # failed steps; reporter-only failures retain legacy deterministic bisection.
    primary =
      Enum.reject(failed, fn step ->
        is_binary(step.name) and
          (String.starts_with?(step.name, "Report build and bump-guard statuses") or
             step.name == "Post the terminal merge-group build status")
      end)

    sandbox_green =
      Enum.any?(jobs, &(&1["name"] == "sandboxed-build" and &1["conclusion"] == "success"))

    cond do
      not valid ->
        :unavailable

      run["conclusion"] == "cancelled" ->
        {:infrastructure, evidence}

      Enum.any?(failed, &(&1.name in @candidate_steps)) ->
        {:conclusive, evidence}

      run["conclusion"] == "timed_out" or Enum.any?(jobs, &(&1["conclusion"] == "timed_out")) ->
        {:conclusive, Map.put(evidence, :diagnostic, "terminal_timeout")}

      interrupted ->
        {:infrastructure, evidence}

      # Optional telemetry can fail under continue-on-error after the verdict.
      # A green workflow cannot establish an infrastructure cause for red scope.
      run["conclusion"] == "success" ->
        {:conclusive, Map.put(evidence, :diagnostic, "unknown_terminal_failure_bisect")}

      # Scope has its own status and can be red even when every job is green.
      # Only a failed workflow after a green sandbox indicates later infra.
      sandbox_green and run["conclusion"] == "failure" ->
        {:infrastructure, evidence}

      primary != [] and
          Enum.all?(primary, fn step ->
            is_binary(step.name) and
              Enum.any?(@infrastructure_prefixes, &String.starts_with?(step.name, &1)) and
                not (step.pin_validation_ran and
                         Enum.any?(
                           [
                             "Fetch ",
                             "Prepare trusted read-only Lean watchdog",
                             "Build the trusted environment-lint driver"
                           ],
                           &String.starts_with?(step.name, &1)
                         ))
          end) ->
        {:infrastructure, evidence}

      true ->
        {:conclusive, Map.put(evidence, :diagnostic, "unknown_terminal_failure_bisect")}
    end
  end

  def classify_jobs(_, _), do: :unavailable
end
