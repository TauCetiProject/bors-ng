defmodule BorsNG.Worker.StatisticsTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.Statistics

  test "windows count terminal outcomes, distinct PR landings and repeated staging appearances" do
    now = 1_800_000_000
    at = fn seconds -> (now - seconds) |> DateTime.from_unix!() |> DateTime.to_naive() end

    batches = [
      %{id: 1, state: :error, updated_at: at.(100)},
      %{id: 2, state: :ok, updated_at: at.(50)},
      %{id: 3, state: :canceled, updated_at: at.(7200)},
      %{id: 4, state: :error, updated_at: at.(90000)}
    ]

    candidates =
      for id <- [1, 2],
          do: %{
            id: id,
            batch_id: id,
            into_branch: "main",
            snapshot: %{"base_sha" => "base", "members" => [%{"pr" => 10}, %{"pr" => 11}]}
          }

    events = [
      %{candidate_id: 1, kind: "dispatched"},
      %{candidate_id: 2, kind: "dispatched"},
      %{candidate_id: 2, kind: "dispatched"},
      %{
        candidate_id: 2,
        kind: "constructed",
        details: %{"commit" => "head", "base_sha" => "base"}
      },
      %{
        candidate_id: 2,
        kind: "promoted",
        source: "github.base_push_response",
        observed_at: (now - 60) * 1_000_000,
        details: %{"commit" => "head", "base_sha" => "base", "base" => "main"}
      }
    ]

    [hour, three, day, week] = Statistics.summarize(batches, candidates, events, now)
    assert hour.completed_batches == 2
    assert hour.successful_batches == 1 and hour.failed_batches == 1
    assert hour.landed_prs == 2
    assert hour.staging_pr_runs == 4 and hour.staging_unique_prs == 2
    assert hour.mean_staging_runs_per_pr == 2.0
    assert three.canceled_batches == 1 and three.batches_with_unknown_membership == 1
    assert day.completed_batches == 3
    assert week.completed_batches == 4
  end

  test "a green status or unbound promotion is not landing proof; missing observations stay unknown" do
    now = 1_800_000_000
    [hour | _] = Statistics.summarize([], [], [], now)
    assert hour.completed_batches == 0 and hour.landed_prs == 0
    assert hour.mean_staging_runs_per_pr == nil
  end
end
