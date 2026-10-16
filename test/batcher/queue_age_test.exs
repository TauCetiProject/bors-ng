defmodule BorsNG.Worker.QueueAgeTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.Batcher.QueueAge

  # Maps keep the production replay executable without Ecto/application/DB startup:
  # elixir -r lib/worker/batcher/queue_age.ex -e 'ExUnit.start(); Code.require_file("test/batcher/queue_age_test.exs")'
  defp batch(id, poll, attrs \\ %{}) do
    Map.merge(
      %{
        id: id,
        project_id: 1,
        into_branch: "main",
        state: :waiting,
        priority: 0,
        last_polled: poll,
        sealed: true,
        recovery_after: 0,
        promotion_retry_at: 0,
        recovery_failures: 0
      },
      attrs
    )
  end

  defp member(batch, id, age, count \\ 0) do
    link = %{
      id: id,
      batch_id: batch.id,
      patch_id: id,
      head_sha: "head#{id}",
      reviewer: "human",
      manual_approval: %{"head_sha" => "head#{id}", "actor_id" => 7}
    }

    admission = %{
      project_id: batch.project_id,
      into_branch: batch.into_branch,
      batch_id: batch.id,
      link_id: id,
      observed_at: age + 1,
      snapshot: %{
        "base" => batch.into_branch,
        "patch_id" => id,
        "head_sha" => link.head_sha,
        "reviewer" => link.reviewer,
        "manual_approval" => link.manual_approval,
        "original_admitted_at" => age,
        "queue_age_retries" => count
      }
    }

    {link, link.head_sha, admission}
  end

  defp ordered(batches, rows, now \\ 2_000_000_000_000_000) do
    keys = QueueAge.keys(batches, rows, now)
    Enum.sort_by(batches, &QueueAge.order_key(&1, keys))
  end

  defp us(timestamp),
    do: timestamp |> DateTime.from_iso8601() |> elem(1) |> DateTime.to_unix(:microsecond)

  test "frozen Oct16 queue replays exactly, retaining running sentinel and all member data" do
    frozen = [
      {854, "2026-10-16T04:26:53Z", "2026-10-14T20:28:37.304621Z", 1},
      {856, "2026-10-16T04:40:11Z", "2026-10-14T19:54:22.766728Z", 2},
      {859, "2026-10-16T05:25:19Z", "2026-10-15T03:42:56.428508Z", 31},
      {860, "2026-10-16T05:49:47Z", "2026-10-16T03:43:28.264589Z", 1},
      {861, "2026-10-16T05:49:47Z", "2026-10-16T02:43:51.967247Z", 51},
      {862, "2026-10-16T05:51:00Z", "2026-10-16T05:51:00.214070Z", 22},
      {863, "2026-10-16T06:35:21Z", "2026-10-15T03:18:56.646146Z", 24}
    ]

    batches = for {id, poll, _, _} <- frozen, do: batch(id, div(us(poll), 1_000_000))
    # Aggregates supply minima/counts, not historical per-head records. Synthetic
    # member identities exercise invariance; this is not a per-head production export.
    rows =
      for {id, _, age, count} <- frozen,
          i <- 1..count,
          do: member(Enum.find(batches, &(&1.id == id)), id * 100 + i, us(age) + i - 1)

    running = batch(852, 0, %{state: :running, priority: 100})
    before = {batches, rows}

    assert batches |> Enum.sort_by(& &1.last_polled) |> Enum.map(& &1.id) == [
             854,
             856,
             859,
             860,
             861,
             862,
             863
           ]

    assert ordered([running | Enum.reverse(batches)], rows) |> Enum.map(& &1.id) == [
             852,
             856,
             854,
             863,
             859,
             861,
             860,
             862
           ]

    assert {batches, rows} == before
    assert length(rows) == 132
  end

  test "explicit priorities dominate age; running wins even over priority above100" do
    older = batch(1, 10)
    priority = batch(2, 20, %{priority: 101})
    running = batch(3, 30, %{state: :running, priority: 100})

    assert ordered([older, priority, running], [member(older, 1, 1), member(priority, 2, 2)]) == [
             running,
             priority,
             older
           ]
  end

  test "equal microsecond age ties use poll time then durable batch ID, independent of SQL order" do
    a = batch(1, 10)
    b = batch(2, 10)
    c = batch(3, 11)
    rows = [member(a, 1, 1), member(b, 2, 1), member(c, 3, 1)]
    assert ordered([c, b, a], Enum.reverse(rows)) == [a, b, c]
  end

  test "missing, malformed, future and mismatched metadata falls back for the entire group" do
    b = batch(1, 20)
    good = member(b, 1, 1)
    {link, head, a} = member(b, 2, 2)

    invalid = [
      nil,
      %{a | project_id: 2},
      %{a | into_branch: "other"},
      %{a | batch_id: 2},
      %{a | link_id: 3},
      %{a | observed_at: 1},
      %{a | observed_at: 101}
    ]

    snapshots =
      for {k, v} <- [
            {"original_admitted_at", nil},
            {"original_admitted_at", "1"},
            {"original_admitted_at", -1},
            {"head_sha", "changed"},
            {"patch_id", 3},
            {"base", "other"},
            {"reviewer", "other"},
            {"manual_approval", nil},
            {"queue_age_retries", -1},
            {"queue_age_retries", "0"}
          ],
          do: %{a | snapshot: Map.put(a.snapshot, k, v)}

    for bad <- invalid ++ snapshots do
      assert QueueAge.key(b, [good, {link, head, bad}], 100) == {20_000_000, :poll_fallback}
    end

    assert QueueAge.key(b, [good, {link, "changed", a}], 100) == {20_000_000, :poll_fallback}
    assert QueueAge.key(b, [good, good], 100) == {20_000_000, :poll_fallback}
    assert QueueAge.key(b, [], 100) == {20_000_000, :poll_fallback}
  end

  test "withdrawal removes the oldest age; bundles/sealed groups remain whole" do
    b = batch(1, 20)
    older = member(b, 1, 1)
    younger = member(b, 2, 2)
    assert QueueAge.key(b, [older, younger], 100) == {1, :original_admission}
    assert QueueAge.key(b, [younger], 100) == {2, :original_admission}
    assert ordered([b], [older, younger]) == [b]
  end

  test "retry generations preserve age initially then yield behind waiting peers indefinitely" do
    peer = batch(1, 10)
    assert QueueAge.next_retry(nil) == 3
    assert QueueAge.next_retry(%{"queue_age_retries" => "bad"}) == 3

    for generation <- 0..12 do
      retry = batch(2, 20 + generation)

      count =
        Enum.reduce(1..generation//1, 0, fn _, count ->
          QueueAge.next_retry(%{"queue_age_retries" => count})
        end)

      # Empty range for generation0 retains the original admission.
      assert count == min(generation, 3)
      rows = [member(peer, 1, 5), member(retry, 2, 1, count)]

      assert ordered([retry, peer], rows) ==
               if(generation < 3, do: [retry, peer], else: [peer, retry])
    end

    b = batch(2, 20, %{recovery_failures: 3})
    assert QueueAge.key(b, [member(b, 2, 1)], 100) == {20_000_000, :retry_fairness}
  end

  test "legacy journal grace is bounded without changing original timestamp" do
    b = batch(1, 20)
    {l, h, a} = member(b, 1, 1)
    legacy = Map.delete(a.snapshot, "queue_age_retries")
    assert QueueAge.key(b, [{l, h, %{a | snapshot: legacy}}], 100) == {1, :original_admission}
    assert QueueAge.next_retry(legacy) == 1
  end

  test "poll delay, recovery and promotion deadlines remain independent eligibility gates" do
    b = batch(1, 20)
    assert QueueAge.eligible?(b, 30, true)
    refute QueueAge.eligible?(b, 30, false)
    refute QueueAge.eligible?(%{b | state: :running}, 30, true)
    refute QueueAge.eligible?(%{b | recovery_after: 31}, 30, true)
    refute QueueAge.eligible?(%{b | promotion_retry_at: 31}, 30, true)
    assert QueueAge.eligible?(%{b | recovery_after: 30, promotion_retry_at: 30}, 30, true)
    assert QueueAge.key(b, [member(b, 1, 1)], 100) == {1, :original_admission}
    assert b.last_polled == 20
  end
end
