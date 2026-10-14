defmodule BorsNG.StagingFailureTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.StagingFailure

  @head String.duplicate("a", 40)
  @base String.duplicate("b", 40)
  @repo "TauCetiProject/TauCeti"

  defp status do
    %{
      "sha" => @head,
      "statuses" => [
        %{
          "context" => "build",
          "state" => "failure",
          "creator" => %{"id" => 41_898_282},
          "target_url" => "https://github.com/#{@repo}/actions/runs/123"
        }
      ]
    }
  end

  defp run do
    %{
      "event" => "repository_dispatch",
      "path" => ".github/workflows/pr-build.yml",
      "display_title" => "bors branch=main batch=7 head=#{@head} base=#{@base}",
      "status" => "completed",
      "conclusion" => "cancelled"
    }
  end

  test "only the latest trusted exact-head build selects a run" do
    assert StagingFailure.build_source(status(), @head, @repo) == {:lookup, 123}
    assert StagingFailure.build_source(status(), @base, @repo) == :unavailable
    assert StagingFailure.build_source(status(), @head, "another/repo") == :unavailable

    for value <- ["success", "pending"] do
      data = put_in(status(), ["statuses", Access.at(0), "state"], value)
      assert StagingFailure.build_source(data, @head, @repo) == :pending
    end

    for url <- [
          "https://attacker.example/#{@repo}/actions/runs/123",
          "http://github.com/#{@repo}/actions/runs/123",
          "https://github.com/#{@repo}/actions/runs/123/extra",
          "https://github.com/#{@repo}/actions/runs/123?other=1"
        ] do
      data = put_in(status(), ["statuses", Access.at(0), "target_url"], url)
      assert StagingFailure.build_source(data, @head, @repo) == :unavailable
    end

    data = put_in(status(), ["statuses", Access.at(0), "creator", "id"], 123)
    assert StagingFailure.build_source(data, @head, @repo) == :unavailable
    assert StagingFailure.build_source(%{}, @head, @repo) == :unavailable
  end

  test "run identity includes workflow, event, branch, batch, head and base" do
    assert StagingFailure.classify_run(run(), 7, @head, "main", @base) == :cancelled

    for {key, value} <- [
          {"event", "pull_request_target"},
          {"path", ".github/workflows/other.yml"},
          {"display_title", "bors branch=main batch=8 head=#{@head} base=#{@base}"}
        ] do
      assert StagingFailure.classify_run(Map.put(run(), key, value), 7, @head, "main", @base) ==
               :unavailable
    end

    assert StagingFailure.classify_run(run(), 7, @base, "main", @base) == :unavailable
    assert StagingFailure.classify_run(run(), 7, @head, "other", @base) == :unavailable
    assert StagingFailure.classify_run(run(), 7, @head, "main", @head) == :unavailable

    for conclusion <- ["success", "failure", "timed_out"] do
      assert StagingFailure.classify_run(
               Map.put(run(), "conclusion", conclusion),
               7,
               @head,
               "main",
               @base
             ) ==
               :conclusive
    end

    assert StagingFailure.classify_run(
             Map.put(run(), "status", "in_progress"),
             7,
             @head,
             "main",
             @base
           ) ==
             :pending
  end

  test "early guard failure binds its run while build is still pending" do
    [build] = status()["statuses"]

    for context <- ["scope", "bump-guard"] do
      data = %{
        "sha" => @head,
        "statuses" => [Map.put(build, "state", "pending"), Map.put(build, "context", context)]
      }

      assert StagingFailure.build_source(data, @head, @repo) == {:lookup, 123}
    end
  end

  test "a cancelled older native attempt is propagation, not a new cancellation" do
    assert StagingFailure.classify_run(run(), 8, @head, "main", @base) == :pending
  end

  test "job conclusions classify infrastructure without trusting log or metric text" do
    run = %{"id" => 123, "run_attempt" => 1, "conclusion" => "failure"}

    job = %{
      "id" => 456,
      "run_id" => 123,
      "status" => "completed",
      "conclusion" => "failure",
      "steps" => []
    }

    wrap = fn job -> %{"total_count" => 1, "jobs" => [job]} end

    assert {:conclusive, %{diagnostic: "unknown_terminal_failure_bisect"}} =
             StagingFailure.classify_jobs(run, wrap.(job))

    candidate_step = %{
      "name" => "Build exact candidate under bwrap with workflow-pinned audits, offline",
      "number" => 15,
      "conclusion" => "failure"
    }

    failed = Map.put(job, "steps", [candidate_step])
    assert {:conclusive, _} = StagingFailure.classify_jobs(run, wrap.(failed))

    for state <- ["cancelled", "timed_out", "stale", "action_required"] do
      assert {:conclusive, _} =
               StagingFailure.classify_jobs(run, wrap.(Map.put(failed, "conclusion", state)))
    end

    assert {:conclusive, _} =
             StagingFailure.classify_jobs(Map.put(run, "conclusion", "timed_out"), wrap.(failed))

    assert {:infrastructure, _} =
             StagingFailure.classify_jobs(Map.put(run, "conclusion", "cancelled"), wrap.(failed))

    assert {:conclusive, _} =
             StagingFailure.classify_jobs(run, %{
               "total_count" => 2,
               "jobs" => [failed, %{job | "id" => 457, "conclusion" => "cancelled"}]
             })

    assert :unavailable =
             StagingFailure.classify_jobs(run, %{"total_count" => 2, "jobs" => [failed]})

    assert :unavailable = StagingFailure.classify_jobs(run, wrap.(Map.put(failed, "run_id", 999)))

    setup =
      Map.put(job, "steps", [
        %{candidate_step | "name" => "Install elan (trusted, pinned + checksum)"}
      ])

    assert {:infrastructure, _} = StagingFailure.classify_jobs(run, wrap.(setup))
  end

  test "red verdict reporters cannot disguise config failures as infrastructure" do
    run = %{"id" => 123, "run_attempt" => 1, "conclusion" => "failure"}
    step = fn name -> %{"name" => name, "conclusion" => "failure", "number" => 1} end

    job = %{
      "run_id" => 123,
      "id" => 1,
      "status" => "completed",
      "name" => "sandboxed-build",
      "conclusion" => "failure",
      "steps" => [step.("Report build and bump-guard statuses to the PR head commit")]
    }

    finalizer = %{
      job
      | "id" => 2,
        "name" => "finalize-merge-group-build",
        "steps" => [step.("Post the terminal merge-group build status")]
    }

    wrap = fn jobs -> %{"total_count" => length(jobs), "jobs" => jobs} end

    assert {:conclusive, %{diagnostic: "unknown_terminal_failure_bisect"}} =
             StagingFailure.classify_jobs(run, wrap.([job, finalizer]))

    setup = %{
      job
      | "steps" => [step.("Install elan (trusted, pinned + checksum)") | job["steps"]]
    }

    assert {:infrastructure, _} = StagingFailure.classify_jobs(run, wrap.([setup, finalizer]))

    metadata = %{
      job
      | "steps" => [step.("Prepare merge-group cache artifact metadata") | job["steps"]]
    }

    assert {:conclusive, _} = StagingFailure.classify_jobs(run, wrap.([metadata, finalizer]))
    # Reusable publisher step names may change independently. A fully green
    # sandbox followed by publication/finalization failure remains infrastructure.
    publisher = %{
      job
      | "id" => 3,
        "name" => "publish-merge-group-cache / publish",
        "steps" => [step.("Publish exact revision map")]
    }

    green = %{job | "conclusion" => "success", "steps" => []}

    assert {:infrastructure, _} =
             StagingFailure.classify_jobs(run, wrap.([green, publisher, finalizer]))
  end
end
