defmodule BorsNG.Worker.Batcher.DiversityReplay do
  @moduledoc "Offline formation sensitivity; deliberately has no outcome oracle."
  alias BorsNG.Worker.Batcher.Diversity

  def run(data) do
    scenarios =
      for coverage <- [:as_of, :retrospective, :retrospective_known],
          bound <- [:lo, :hi],
          cap <- [8, 16, 32, 128],
          scale <- [1, 4],
          mode <- [:fifo, :diversify, :group, :cap2, :cap3] do
        replay(data, coverage, bound, cap, scale, mode)
      end

    %{
      schema: "bors.diversity-replay/v1",
      production_enabled: false,
      recommendation: "retain existing policy; no identified accepted-throughput benefit",
      source:
        Map.take(data, [
          "source_repository",
          "source_commit",
          "evidence_source",
          "snapshots",
          "start",
          "end",
          "labels_observed_at",
          "limits"
        ]),
      scenarios: scenarios,
      observed_candidates: data["observed_candidates"],
      concrete_interaction: data["concrete_interaction"],
      causal_metrics: %{
        accepted_prs_per_hour: nil,
        retry_amplification: nil,
        runner_minutes_per_accepted_pr: nil,
        cpu_seconds_per_accepted_pr: nil,
        reason:
          "Counterfactual formations are unbuilt. No outcome is assigned to unknown pairs or failed groups."
      }
    }
  end

  def replay(data, coverage, bound, cap, scale, mode) do
    first = data["start"]

    units =
      data["heads"]
      |> Enum.map(fn h ->
        at = (h[Atom.to_string(bound)] - first) / scale
        stale = if h["stale_at"], do: (h["stale_at"] - first) / scale
        label_at = (data["labels_observed_at"] - first) / scale

        hint =
          if coverage != :as_of or label_at <= at,
            do: Diversity.hint(1, h["pr"], h["head_sha"], h["labels"], trunc(label_at))

        %{
          id: "#{h["pr"]}@#{h["head_sha"]}",
          priority: 0,
          admitted_at: at,
          eligible_at: at,
          stale_at: stale,
          work: 1,
          fresh: true,
          singleton: false,
          members: [%{pr: h["pr"], head_sha: h["head_sha"], label_hint: hint}]
        }
      end)
      |> Enum.filter(fn u ->
        coverage != :retrospective_known or
          MapSet.size(
            Diversity.areas(hd(u.members), 1, trunc((data["labels_observed_at"] - first) / scale))
          ) > 0
      end)
      |> Enum.sort_by(&{&1.eligible_at, &1.id})

    # Fixed 600s is a service sensitivity, not a fitted build model. Work is
    # explicitly imputed to one/member. There is no wait-to-fill or failure draw.
    {trace, invalidated} = drain(units, [], 0, %{}, [], [], coverage, cap, mode, 600)
    selected = Enum.flat_map(trace, & &1.members)
    waits = Enum.flat_map(trace, & &1.waits_s) |> Enum.sort()

    %{
      coverage: coverage,
      admission_bound: bound,
      cap: cap,
      arrival_multiplier: scale,
      mode: mode,
      service_s_assumption: 600,
      work: "imputed one/member; physical work unknown",
      priority: "unknown; equal-priority assumption only",
      atomic_units: "unknown; standalone assumption only",
      input_heads: length(units),
      attempted_heads: length(selected),
      invalidated_heads: length(invalidated),
      batches: length(trace),
      same_area_pairs: Enum.sum(Enum.map(trace, & &1.same_area_pairs)),
      p90_dispatch_wait_s: quantile(waits, 0.9),
      max_dispatch_wait_s: List.last(waits),
      simulated_lane_minutes: length(trace) * 10,
      accepted_prs_per_hour: nil,
      retry_amplification: nil,
      runner_minutes_per_accepted_pr: nil,
      invalidated: invalidated,
      formations: trace
    }
  end

  defp drain([], [], _, _, trace, invalid, _, _, _, _), do: {Enum.reverse(trace), invalid}

  defp drain(future, queue, now, bypass, trace, invalid, coverage, cap, mode, service) do
    now = if queue == [] and future != [], do: max(now, hd(future).eligible_at), else: now
    {arrived, future} = Enum.split_while(future, &(&1.eligible_at <= now))
    visible = queue ++ arrived

    latest =
      visible
      |> Enum.group_by(&hd(&1.members).pr)
      |> Map.new(fn {pr, us} ->
        {pr, Enum.max_by(us, &{&1.eligible_at, &1.id}).id}
      end)

    {expired, queue} =
      Enum.split_with(visible, fn u ->
        (u.stale_at != nil and u.stale_at <= now) or latest[hd(u.members).pr] != u.id
      end)

    invalid = invalid ++ Enum.map(expired, & &1.id)

    if queue == [] do
      drain(future, [], now, bypass, trace, invalid, coverage, cap, mode, service)
    else
      # Retrospective sensitivities intentionally expose labels only within
      # this separate diagnostic mode. They never purport to be prospective.
      queue =
        if coverage == :as_of,
          do: queue,
          else:
            Enum.map(queue, fn u ->
              %{
                u
                | members:
                    Enum.map(u.members, fn m ->
                      if m.label_hint,
                        do: %{m | label_hint: Map.put(m.label_hint, "observed_at", trunc(now))},
                        else: m
                    end)
              }
            end)

      {:ok, result} =
        Diversity.select(queue,
          enabled: true,
          mode: mode,
          now: trunc(now),
          project_id: 1,
          cap: cap,
          lookahead: 256,
          bypass: bypass
        )

      members = Enum.flat_map(result.selected, & &1.members)
      composition = for m <- members, a <- Diversity.areas(m, 1, trunc(now)), do: a

      row = %{
        dispatched_at_s: now,
        members: Enum.map(result.selected, & &1.id),
        areas: Enum.frequencies(composition),
        unknown_area_members:
          Enum.count(members, &(MapSet.size(Diversity.areas(&1, 1, trunc(now))) == 0)),
        size: length(members),
        imputed_work: length(members),
        waits_s: Enum.map(result.selected, &(now - &1.eligible_at)),
        same_area_pairs: Diversity.pairs(members, 1, trunc(now)),
        fallback: result.fallback,
        failure_cause: "unobserved; no counterfactual CI or assembly result"
      }

      drain(
        future,
        result.remaining,
        now + service,
        result.bypass,
        [row | trace],
        invalid,
        coverage,
        cap,
        mode,
        service
      )
    end
  end

  defp quantile([], _), do: nil
  defp quantile(xs, q), do: Enum.at(xs, trunc((length(xs) - 1) * q))
end
