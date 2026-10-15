defmodule BorsNG.Worker.Batcher.Diversity do
  @moduledoc """
  Offline, disabled-by-default fresh formation planner. No scheduler calls this
  module. Inputs are already eligible atomic units in one repository/base lane;
  it never grants eligibility or edits queue/candidate membership.

  Units carry id, priority, admitted_at (seconds), members, work, fresh and
  singleton. Members carry pr, head_sha and an optional label_hint. Mutable
  hints are scheduling evidence only. Every formation anchors the oldest unit
  at the highest exact priority. See deploy/batch-diversity.md before integration.
  """

  @ttl 900

  def hint(project_id, pr, head, labels, at) do
    if is_list(labels) and length(labels) <= 100 and
         Enum.all?(labels, &(is_binary(&1) and byte_size(&1) <= 256)) do
      %{
        "schema" => "bors.scheduling-label-hint/v1",
        "timestamp_unit" => "unix_seconds",
        "project_id" => project_id,
        "pr" => pr,
        "head_sha" => head,
        "observed_at" => at,
        "source" => "github.get_labels/preflight",
        "binding" => "local preflight head; PR labels are mutable, not an atomic head read",
        "labels" => Enum.sort(Enum.uniq(labels))
      }
    end
  end

  # A per-mutator scratch buffer, bounded independently of queue size. It is
  # expendable on restart and never consulted by approval or source checks.
  def remember(patch, labels) do
    now = System.system_time(:second)
    key = {patch.project_id, patch.pr_xref, patch.commit}
    value = hint(patch.project_id, patch.pr_xref, patch.commit, labels, now)

    cache =
      Process.get(__MODULE__, %{})
      |> Enum.filter(fn {_, h} -> (now - h["observed_at"]) in 0..@ttl end)
      |> Map.new()
      |> Map.delete(key)

    cache = if value, do: Map.put(cache, key, value), else: cache

    Process.put(
      __MODULE__,
      cache
      |> Enum.sort_by(fn {k, h} -> {-h["observed_at"], k} end)
      |> Enum.take(256)
      |> Map.new()
    )

    :ok
  end

  def cached(patch, now \\ System.system_time(:second)) do
    h = Process.get(__MODULE__, %{})[{patch.project_id, patch.pr_xref, patch.commit}]
    if h && (now - h["observed_at"]) in 0..@ttl, do: h
  end

  def areas(member, project_id, now) do
    case Map.get(member, :label_hint) do
      %{
        "schema" => "bors.scheduling-label-hint/v1",
        "timestamp_unit" => "unix_seconds",
        "project_id" => ^project_id,
        "pr" => pr,
        "head_sha" => head,
        "observed_at" => at,
        "source" => "github.get_labels/preflight",
        "labels" => labels
      }
      when pr == member.pr and head == member.head_sha and is_integer(at) and
             now - at >= 0 and now - at <= @ttl and is_list(labels) ->
        labels
        |> Enum.filter(&is_binary/1)
        |> Enum.filter(&String.starts_with?(&1, "roadmap/"))
        |> Enum.map(&String.replace_prefix(&1, "roadmap/", ""))
        |> Enum.reject(&(String.downcase(&1) in ["", "none", "unknown"]))
        |> MapSet.new()

      _ ->
        MapSet.new()
    end
  end

  def select(units, opts \\ []) do
    cap = Keyword.get(opts, :cap, 128)
    lookahead = Keyword.get(opts, :lookahead, 64)

    mode =
      if Keyword.get(opts, :enabled, false), do: Keyword.get(opts, :mode, :diversify), else: :fifo

    now = Keyword.fetch!(opts, :now)
    project = Keyword.fetch!(opts, :project_id)
    bypass = Map.take(Keyword.get(opts, :bypass, %{}), Enum.map(units, & &1.id))
    max_bypass = Keyword.get(opts, :max_bypass, 3)
    work_limit = Keyword.get(opts, :max_work, :infinity)

    cond do
      cap not in 1..128 or lookahead not in 1..256 or max_bypass not in 0..32 or
        mode not in [:fifo, :diversify, :group, :cap2, :cap3] or
          not (work_limit == :infinity or (is_number(work_limit) and work_limit > 0)) ->
        {:error, :invalid_bounds}

      Enum.any?(units, &(Map.get(&1, :fresh) != true)) ->
        {:error, :immutable_membership}

      Enum.any?(units, &(&1.members == [] or &1.work <= 0)) ->
        {:error, :invalid_unit}

      duplicate?(Enum.map(units, & &1.id)) or
          duplicate?(for u <- units, m <- u.members, do: m.pr) ->
        {:error, :duplicate_membership}

      units == [] ->
        {:ok, %{selected: [], remaining: [], bypass: bypass, fallback: nil}}

      true ->
        priority = units |> Enum.map(& &1.priority) |> Enum.max()

        lane =
          units
          |> Enum.filter(&(&1.priority == priority))
          |> Enum.sort_by(&{&1.admitted_at, &1.id})

        fifo = fill(lane, [], cap, work_limit, fn _, _ -> 0 end)
        window = Enum.take(lane, max(lookahead, length(fifo)))

        uncertain =
          Enum.any?(window, fn u ->
            Enum.any?(u.members, &(MapSet.size(areas(&1, project, now)) == 0))
          end)

        area_index = Map.new(for u <- window, m <- u.members, do: {m.pr, areas(m, project, now)})

        score = fn u, chosen ->
          forced = Map.get(bypass, u.id, 0) >= max_bypass

          pair_count =
            pairs(u.members, project, now) +
              Enum.sum(
                for m <- u.members, prior <- chosen, other <- prior.members do
                  if MapSet.disjoint?(area_index[m.pr], area_index[other.pr]), do: 0, else: 1
                end
              )

          concentration =
            case mode do
              :group -> -pair_count
              :cap2 -> excess(chosen ++ [u], project, now, 2)
              :cap3 -> excess(chosen ++ [u], project, now, 3)
              _ -> pair_count
            end

          {not forced, concentration, u.admitted_at, u.id}
        end

        proposed =
          if mode == :fifo or uncertain,
            do: fifo,
            else: fill(window, [], cap, work_limit, score)

        # Atomic sizes and isolation can make a greedy alternative underfill.
        # In that case use the reference formation in its entirety.
        forced_missing =
          Enum.any?(fifo, fn u ->
            Map.get(bypass, u.id, 0) >= max_bypass and not Enum.any?(proposed, &(&1.id == u.id))
          end)

        selected =
          if(count(proposed) == count(fifo) and not forced_missing, do: proposed, else: fifo)
          |> Enum.sort_by(&{&1.admitted_at, &1.id})

        ids = MapSet.new(selected, & &1.id)
        remaining = Enum.reject(units, &MapSet.member?(ids, &1.id))

        # Count policy bypasses of reference-eligible work, not unavoidable
        # atomic packing gaps. A due reference unit cannot be skipped again.
        skipped = Enum.reject(fifo, &MapSet.member?(ids, &1.id))

        next =
          Enum.reduce(skipped, Map.drop(bypass, MapSet.to_list(ids)), fn u, b ->
            Map.update(b, u.id, 1, &(&1 + 1))
          end)

        {:ok,
         %{
           selected: selected,
           remaining: remaining,
           bypass: next,
           fallback:
             cond do
               mode == :fifo -> :disabled_or_fifo
               uncertain -> :unknown_or_stale_labels
               count(proposed) != count(fifo) -> :fill_preserved
               forced_missing -> :maximum_bypass
               true -> nil
             end
         }}
    end
  end

  defp duplicate?(xs), do: length(xs) != MapSet.size(MapSet.new(xs))
  defp count(us), do: Enum.sum(Enum.map(us, &length(&1.members)))

  defp fill([], chosen, _, _, _), do: chosen

  defp fill([anchor | rest], [], cap, work, score) do
    if anchor.singleton or length(anchor.members) >= cap or
         (is_number(work) and anchor.work >= work),
       do: [anchor],
       else: fill(rest, [anchor], cap, work, score)
  end

  defp fill(units, chosen, cap, work, score) do
    fits =
      Enum.filter(units, fn u ->
        not u.singleton and count(chosen) + length(u.members) <= cap and
          (work == :infinity or Enum.sum(Enum.map(chosen, & &1.work)) + u.work <= work)
      end)

    case fits do
      [] ->
        chosen

      _ ->
        next = Enum.min_by(fits, &score.(&1, chosen))
        fill(Enum.reject(units, &(&1.id == next.id)), chosen ++ [next], cap, work, score)
    end
  end

  def pairs(members, project, now) do
    members
    |> Enum.with_index()
    |> Enum.reduce(0, fn {m, i}, total ->
      total +
        Enum.count(Enum.drop(members, i + 1), fn other ->
          not MapSet.disjoint?(areas(m, project, now), areas(other, project, now))
        end)
    end)
  end

  defp excess(units, project, now, cap) do
    for(u <- units, m <- u.members, a <- areas(m, project, now), do: a)
    |> Enum.frequencies()
    |> Map.values()
    |> Enum.map(&max(0, &1 - cap))
    |> Enum.sum()
  end
end
