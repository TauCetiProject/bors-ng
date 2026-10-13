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
end
