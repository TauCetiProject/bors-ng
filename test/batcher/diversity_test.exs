defmodule BorsNG.Worker.Batcher.DiversityTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.Batcher.{Diversity, DiversityReplay}

  defp unit(id, area \\ "A", attrs \\ %{}) do
    hint = Diversity.hint(1, id, "h#{id}", ["roadmap/#{area}"], 1000)

    Map.merge(
      %{
        id: id,
        priority: 0,
        admitted_at: id,
        work: 1,
        fresh: true,
        singleton: false,
        members: [%{pr: id, head_sha: "h#{id}", label_hint: hint}]
      },
      attrs
    )
  end

  defp select(us, opts \\ []) do
    Diversity.select(us, Keyword.merge([enabled: true, now: 1000, project_id: 1, cap: 2], opts))
  end

  defp ids(result), do: Enum.map(result.selected, & &1.id)

  test "policy is opt-in and deterministic across input order, with stable oldest tie break" do
    us = [unit(1), unit(2), unit(3, "B")]
    assert {:ok, r} = select(us, enabled: false)
    assert ids(r) == [1, 2]
    assert {:ok, r} = Diversity.select(us, now: 1000, project_id: 1, cap: 2)
    assert ids(r) == [1, 2]
    assert select(us) == select(Enum.reverse(us))
    assert {:ok, r} = select(us)
    assert ids(r) == [1, 3]
    assert r.remaining == [unit(2)]
  end

  test "grouping is tested in the opposite direction; caps remain soft" do
    us = [unit(1), unit(2, "B"), unit(3)]
    assert {:ok, r} = select(us, mode: :group)
    assert ids(r) == [1, 3]

    for mode <- [:fifo, :diversify, :group, :cap2, :cap3] do
      assert {:ok, r} = select(for(i <- 1..200, do: unit(i)), cap: 128, mode: mode)
      assert ids(r) == Enum.to_list(1..128)
    end
  end

  test "exact priority wins over age and concentration, even at extreme values" do
    assert {:ok, r} =
             select([
               unit(1),
               unit(2, "A", %{priority: 2_147_483_647}),
               unit(3, "B", %{priority: -2_147_483_648})
             ])

    assert ids(r) == [2]
  end

  test "lookahead bounds reordering and never adds fill delay" do
    us = [unit(1), unit(2), unit(3), unit(4, "B")]
    assert {:ok, r} = select(us, lookahead: 3)
    assert ids(r) == [1, 2]
    assert {:ok, r} = select(us, lookahead: 4)
    assert ids(r) == [1, 4]
    assert {:ok, r} = select([unit(1)], cap: 128)
    assert ids(r) == [1]
  end

  test "unknown, none, stale, future and missing labels revert the entire window" do
    for member <- [
          hd(unit(2, "Unknown").members),
          hd(unit(2, "none").members),
          %{pr: 2, head_sha: "h2"},
          %{hd(unit(2).members) | label_hint: Diversity.hint(1, 2, "h2", ["roadmap/A"], 99)},
          %{hd(unit(2).members) | label_hint: Diversity.hint(1, 2, "h2", ["roadmap/A"], 1001)}
        ] do
      assert {:ok, r} = select([unit(1), unit(2, "A", %{members: [member]}), unit(3, "B")])
      assert ids(r) == [1, 2]
      assert r.fallback == :unknown_or_stale_labels
    end
  end

  test "provenance binds project, PR and exact head, with inclusive freshness boundary" do
    m = hd(unit(1).members)
    assert m.label_hint["timestamp_unit"] == "unix_seconds"
    assert MapSet.size(Diversity.areas(m, 1, 1900)) == 1
    assert MapSet.size(Diversity.areas(m, 1, 1901)) == 0

    for h <- [
          Map.put(m.label_hint, "project_id", 2),
          Map.put(m.label_hint, "pr", 2),
          Map.put(m.label_hint, "head_sha", "replaced"),
          Map.put(m.label_hint, "source", "unbound"),
          Map.put(m.label_hint, "timestamp_unit", "microseconds")
        ] do
      assert Diversity.areas(%{m | label_hint: h}, 1, 1000) == MapSet.new()
    end
  end

  test "multiple labels count a shared member pair once and deduplicate labels" do
    hint = Diversity.hint(1, 1, "h1", ["roadmap/A", "roadmap/B", "roadmap/A"], 1000)
    m = %{pr: 1, head_sha: "h1", label_hint: hint}
    assert Diversity.pairs([m, hd(unit(2, "A").members), hd(unit(3, "B").members)], 1, 1000) == 2
  end

  test "atomic units and oversized bundles remain intact; singleton isolation remains" do
    bundle = unit(1, "A", %{members: unit(1).members ++ unit(2).members, work: 2})
    assert {:ok, r} = select([bundle, unit(3, "B")], cap: 1)
    assert r.selected == [bundle]
    assert {:ok, r} = select([unit(1, "A", %{singleton: true}), unit(2)])
    assert ids(r) == [1]
    assert {:ok, r} = select([unit(1), unit(2, "B", %{singleton: true}), unit(3, "C")])
    assert ids(r) == [1, 3]
  end

  test "greedy diversity cannot underfill compared with FIFO atomic packing" do
    bundle = unit(2, "A", %{members: unit(2).members ++ unit(3).members, work: 2})
    us = [unit(1), bundle, unit(4, "B")]
    assert {:ok, r} = select(us, cap: 3)
    assert ids(r) == [1, 2]
    assert r.fallback == :fill_preserved
  end

  test "work bounds match the reference and never strand an oversized anchor" do
    us = [unit(1), unit(2), unit(3, "B", %{work: 5})]
    assert {:ok, r} = select(us, max_work: 2)
    assert ids(r) == [1, 2]
    assert {:ok, r} = select([unit(1, "A", %{work: 50}), unit(2)], max_work: 2)
    assert ids(r) == [1]
  end

  test "sealed retries and already constructed inputs are refused; duplicates cannot replace heads" do
    assert select([unit(1, "A", %{fresh: false})]) == {:error, :immutable_membership}
    assert select([unit(1), unit(1)]) == {:error, :duplicate_membership}

    assert select([unit(1), unit(2, "A", %{members: [%{pr: 1, head_sha: "new"}]})]) ==
             {:error, :duplicate_membership}
  end

  test "oldest anchor drains a finite backlog despite same-area bursts" do
    {landed, _, _} =
      Enum.reduce(1..30, {[], for(i <- 1..60, do: unit(i)), %{}}, fn _, {done, us, bypass} ->
        {:ok, r} = select(us, bypass: bypass)
        assert hd(r.selected).id == Enum.min(Enum.map(us, & &1.id))
        {done ++ ids(r), r.remaining, r.bypass}
      end)

    assert Enum.sort(landed) == Enum.to_list(1..60)
  end

  test "maximum bypass forces ordinary work before another diversity preference" do
    us = [unit(1), unit(2), unit(3, "B")]
    assert {:ok, r} = select(us, bypass: %{2 => 3})
    assert ids(r) == [1, 2]
  end

  test "invalid bounds are refused and withdrawn counters do not survive" do
    assert select([unit(1)], cap: 129) == {:error, :invalid_bounds}
    assert select([unit(1)], lookahead: 257) == {:error, :invalid_bounds}
    assert select([unit(1)], max_work: 0) == {:error, :invalid_bounds}
    assert {:ok, r} = select([unit(1)], bypass: %{999 => 3})
    assert r.bypass == %{}
  end

  test "portable eligible-workload strict replay accounts for all heads with no future labels" do
    data = File.read!("test/fixtures/diversity/workload.json") |> Jason.decode!()
    assert length(data["heads"]) == 891
    a = DiversityReplay.replay(data, :as_of, :hi, 128, 1, :fifo)
    b = DiversityReplay.replay(data, :as_of, :hi, 128, 1, :diversify)
    assert Enum.map(a.formations, & &1.members) == Enum.map(b.formations, & &1.members)
    assert b.input_heads == b.attempted_heads + b.invalidated_heads
    assert Enum.all?(b.formations, &(&1.unknown_area_members == &1.size))
  end

  test "label scratch space is bounded, process-local, expendable and exact-head scoped" do
    patch = %{project_id: 1, pr_xref: 1, commit: "h1"}
    Diversity.remember(patch, ["roadmap/A"])
    assert Diversity.cached(patch)["labels"] == ["roadmap/A"]
    assert Diversity.cached(%{patch | commit: "new"}) == nil
    assert Task.async(fn -> Diversity.cached(patch) end) |> Task.await() == nil
    for i <- 1..300, do: Diversity.remember(%{patch | pr_xref: i}, ["roadmap/A"])
    assert map_size(Process.get(Diversity)) == 256
    Process.delete(Diversity)
    assert Diversity.cached(patch) == nil
    assert Diversity.hint(1, 1, "h", List.duplicate("label", 101), 0) == nil
  end

  test "label changes replace observations and expired or future scratch entries fall back" do
    p = %{project_id: 1, pr_xref: 1, commit: "h"}
    Diversity.remember(p, ["roadmap/A"])
    Diversity.remember(p, ["roadmap/B"])
    h = Diversity.cached(p)
    assert h["labels"] == ["roadmap/B"]
    assert Diversity.cached(p, h["observed_at"] + 901) == nil
    assert Diversity.cached(p, h["observed_at"] - 1) == nil
  end

  test "strict replay does not leak retrospective labels and accounts for withdrawn heads" do
    d = %{
      "start" => 1000,
      "labels_observed_at" => 2000,
      "heads" => [
        %{
          "pr" => 1,
          "head_sha" => "h1",
          "lo" => 1000,
          "hi" => 1000,
          "stale_at" => nil,
          "labels" => ["roadmap/A"]
        },
        %{
          "pr" => 2,
          "head_sha" => "h2",
          "lo" => 1001,
          "hi" => 1001,
          "stale_at" => 1002,
          "labels" => ["roadmap/A"]
        },
        %{
          "pr" => 3,
          "head_sha" => "h3",
          "lo" => 1001,
          "hi" => 1001,
          "stale_at" => nil,
          "labels" => ["roadmap/B"]
        }
      ]
    }

    a = DiversityReplay.replay(d, :as_of, :hi, 2, 1, :fifo)
    b = DiversityReplay.replay(d, :as_of, :hi, 2, 1, :diversify)

    assert a.formations == b.formations or
             Enum.map(a.formations, & &1.members) == Enum.map(b.formations, & &1.members)

    assert b.invalidated == ["2@h2"]
    assert b.attempted_heads + b.invalidated_heads == b.input_heads
    assert b.accepted_prs_per_hour == nil
    assert b.retry_amplification == nil
    assert b == DiversityReplay.replay(d, :as_of, :hi, 2, 1, :diversify)
  end
end
