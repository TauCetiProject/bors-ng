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

  defp remainder(b, n, age, count \\ 1) do
    for i <- 1..n do
      {l, head, a} = member(b, b.id * 1000 + i, age + i)

      {l, head,
       %{
         a
         | snapshot:
             Map.merge(a.snapshot, %{
               "source" => "bors.retry",
               "retry_reason" => "provisional_remainder",
               "parent_candidate_id" => 99,
               "remainder_retries" => count
             })
       }}
    end
  end

  test "larger exact diagnostic remainders precede ordinary work, after explicit priority and running" do
    running = batch(1, 10, %{state: :running, priority: 100})
    installer = batch(2, 20, %{priority: 101})
    ordinary = batch(3, 10)
    small = batch(4, 30)
    large = batch(5, 40)

    rows =
      [member(ordinary, 3, 1), member(installer, 2, 2)] ++
        remainder(small, 2, 10) ++ remainder(large, 3, 20)

    assert ordered([small, large, ordinary, installer, running], rows) ==
             [running, installer, large, small, ordinary]

    # A changed head or missing admission removes preference for the whole group.
    [{link, _, admission} | rest] = remainder(large, 3, 20)

    assert ordered([large, ordinary], [member(ordinary, 3, 1), {link, "moved", admission} | rest]) ==
             [ordinary, large]

    assert ordered([large, ordinary], [member(ordinary, 3, 1) | remainder(large, 3, 20, 4)]) ==
             [ordinary, large]

    refute QueueAge.eligible?(%{large | recovery_after: 101}, 100, true)
  end

  test "neutral cloning cannot renew a remainder preference after three removals" do
    count =
      Enum.reduce(1..10, 0, fn _, count ->
        count = QueueAge.next_remainder(%{"remainder_retries" => count}, "diagnostic_remainder")
        QueueAge.next_remainder(%{"remainder_retries" => count}, "train_invalidated")
      end)

    assert count == 4
    assert QueueAge.next_remainder(nil, "diagnostic_remainder") == 4
  end

  test "located removal earns bounded preference after old conflicts exhausted generic retry age" do
    ordinary = batch(1, 10)
    retained = batch(2, 20)

    for count <- 0..4 do
      rows =
        remainder(retained, 3, 20, count)
        |> Enum.map(fn {l, h, a} ->
          {l, h, %{a | snapshot: Map.put(a.snapshot, "queue_age_retries", 3)}}
        end)

      keys = QueueAge.keys([ordinary, retained], [member(ordinary, 1, 1) | rows], 100)

      assert Enum.sort_by([ordinary, retained], &QueueAge.order_key(&1, keys)) ==
               if(count <= 3, do: [retained, ordinary], else: [ordinary, retained])

      assert elem(Map.fetch!(keys, retained.id), 0) |> is_integer()
    end
  end

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

  test "failed retries preserve age initially then yield behind already waiting peers" do
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
    assert QueueAge.key(b, [member(b, 2, 1)], 100) == {1, :original_admission}
  end

  test "neutral clones and manual approval changes preserve unchanged head age" do
    for reason <-
          ~w(train_invalidated stale_dispatch member_removed cancellation non_fast_forward provisional_remainder provisional_unconfirmed) do
      count =
        Enum.reduce(1..10, 0, fn _, count ->
          QueueAge.next_retry(%{"queue_age_retries" => count}, reason)
        end)

      assert count == 0
      assert QueueAge.next_retry(%{"queue_age_retries" => 3}, reason) == 3
    end

    b = batch(1, 20)
    {l, h, a} = member(b, 1, 1)
    assert QueueAge.key(b, [{%{l | manual_approval: nil}, h, a}], 100) == {1, :original_admission}

    assert QueueAge.key(
             b,
             [{l, h, %{a | snapshot: Map.put(a.snapshot, "manual_approval", nil)}}],
             100
           ) == {1, :original_admission}
  end

  test "legacy journal grace is bounded without changing original timestamp" do
    b = batch(1, 20)
    {l, h, a} = member(b, 1, 1)
    legacy = Map.delete(a.snapshot, "queue_age_retries")
    assert QueueAge.key(b, [{l, h, %{a | snapshot: legacy}}], 100) == {1, :original_admission}
    assert QueueAge.next_retry(legacy) == 1
  end

  test "a handed-off suspect inherits its retained age once, only at a different head" do
    suspect = %{
      "pr" => 7,
      "head_sha" => "old",
      "original_admitted_at" => 50,
      "queue_age_retries" => 1
    }

    assert QueueAge.readmission(suspect, "new", 100) == {50, 1}
    # Same head: existing request rules apply, nothing is inherited.
    assert QueueAge.readmission(suspect, "old", 100) == nil
    # Legacy record without a counter starts with the full allowance.
    assert QueueAge.readmission(Map.delete(suspect, "queue_age_retries"), "new", 100) == {50, 0}
    # The allowance travels unchanged; an exhausted one has nothing to pass on.
    assert QueueAge.readmission(%{suspect | "queue_age_retries" => 2}, "new", 100) == {50, 2}
    assert QueueAge.readmission(%{suspect | "queue_age_retries" => 3}, "new", 100) == nil

    for bad <- [
          %{suspect | "original_admitted_at" => nil},
          %{suspect | "original_admitted_at" => 0},
          %{suspect | "original_admitted_at" => -5},
          %{suspect | "original_admitted_at" => "50"},
          %{suspect | "original_admitted_at" => 101},
          %{suspect | "head_sha" => nil},
          %{suspect | "queue_age_retries" => -1},
          %{suspect | "queue_age_retries" => "0"},
          Map.delete(suspect, "head_sha"),
          Map.delete(suspect, "original_admitted_at"),
          nil,
          []
        ] do
      assert QueueAge.readmission(bad, "new", 100) == nil
    end

    assert QueueAge.readmission(suspect, nil, 100) == nil
    assert QueueAge.readmission(suspect, "new", nil) == nil
    # The age at the observation instant is still valid; a later one is not.
    assert QueueAge.readmission(suspect, "new", 50) == {50, 1}
    assert QueueAge.readmission(suspect, "new", 49) == nil
  end

  test "a readmitted admission is ordered by its inherited age and validated like any other" do
    peer = batch(1, 10)
    readmitted = batch(2, 20)
    {l, h, a} = member(readmitted, 2, 1, 1)

    inherited =
      Map.put(a.snapshot, "inherited_handoff", %{
        "candidate_id" => 99,
        "event_id" => 500,
        "head_sha" => "handed-off",
        "admission_id" => 400,
        "handed_off_at" => 40
      })

    row = {l, h, %{a | snapshot: inherited}}
    assert ordered([peer, readmitted], [member(peer, 1, 5), row]) == [readmitted, peer]
    assert QueueAge.key(readmitted, [row], 100) == {1, :original_admission}

    # The additive field grants nothing on its own: the current admission
    # record still has to pass every exact-head and age check.
    assert QueueAge.key(readmitted, [{l, "moved", %{a | snapshot: inherited}}], 100) ==
             {20_000_000, :poll_fallback}

    assert QueueAge.key(readmitted, [{l, h, %{a | snapshot: inherited, observed_at: 0}}], 100) ==
             {20_000_000, :poll_fallback}

    exhausted = {l, h, %{a | snapshot: Map.put(inherited, "queue_age_retries", 3)}}
    assert QueueAge.key(readmitted, [exhausted], 100) == {20_000_000, :retry_fairness}
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
