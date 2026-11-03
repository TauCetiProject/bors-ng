defmodule BorsNG.Worker.Batcher.QueueAge do
  @moduledoc """
  Pure waiting-group FIFO policy. Only complete, exact current-member admissions
  supply age. Poll timestamps remain operational; see deploy/queue-age.md.
  """

  @inherited_retries 3
  @remainder_reasons ~w(provisional_remainder diagnostic_remainder)

  # A head gets at most three preferred remainder attempts. Repeated failures
  # cannot renew this scheduling preference through neutral clone operations.
  def next_remainder(snapshot, reason) do
    count = if is_map(snapshot), do: Map.get(snapshot, "remainder_retries", 0), else: 4

    if is_integer(count) and count >= 0,
      do: min(count + if(reason in @remainder_reasons, do: 1, else: 0), 4),
      else: 4
  end

  # Neutral topology/membership changes cannot spend the unchanged survivors'
  # failure allowance. Unknown reasons conservatively consume it.
  @neutral_retries ~w(train_invalidated stale_dispatch member_removed cancellation non_fast_forward provisional_remainder provisional_unconfirmed merge_conflict_deferred)

  # Old journals have no counter: allow bounded rollout grace, then persist
  # failed/inconclusive retries. Saturation cannot renew the preference.
  def next_retry(snapshot, reason \\ "retry")
  def next_retry(nil, _reason), do: @inherited_retries

  def next_retry(snapshot, reason) do
    case Map.get(snapshot, "queue_age_retries", 0) do
      count when is_integer(count) and count >= 0 ->
        min(count + if(reason in @neutral_retries, do: 0, else: 1), @inherited_retries)

      _ ->
        @inherited_retries
    end
  end

  def keys(batches, rows, now) do
    members = Enum.group_by(rows, fn {link, _, _} -> link.batch_id end)

    Map.new(batches, fn batch ->
      {batch.id, key(batch, Map.get(members, batch.id, []), now)}
    end)
  end

  def key(batch, rows, now) do
    ages = Enum.map(rows, &member_age(batch, &1, now))
    ids = Enum.map(rows, fn {link, _, _} -> link.id end)

    cond do
      rows == [] or length(Enum.uniq(ids)) != length(ids) or :unknown in ages ->
        {fallback(batch, now), :poll_fallback}

      preferred_remainder?(batch, rows) ->
        # A newly located removal earns its own bounded allowance even when
        # earlier conflicts/bisections exhausted generic age inheritance.
        age =
          rows |> Enum.map(fn {_, _, a} -> a.snapshot["original_admitted_at"] end) |> Enum.min()

        {age, {:diagnostic_remainder, length(rows)}}

      :exhausted in ages ->
        {fallback(batch, now), :retry_fairness}

      true ->
        {Enum.min(ages), :original_admission}
    end
  end

  defp member_age(batch, {link, current_head, admission}, now) when is_map(admission) do
    snapshot = admission.snapshot || %{}
    age = snapshot["original_admitted_at"]
    count = Map.get(snapshot, "queue_age_retries", 0)
    observed = admission.observed_at

    # ManualApproval.grant/6 legitimately updates queued link approvals in
    # place. That does not change head admission age. Approval validity remains
    # exclusively in normal dispatch/promotion checks, never this policy.
    valid =
      admission.project_id == batch.project_id and admission.into_branch == batch.into_branch and
        admission.batch_id == batch.id and link.batch_id == batch.id and
        admission.link_id == link.id and snapshot["base"] == batch.into_branch and
        snapshot["patch_id"] == link.patch_id and is_binary(link.head_sha) and
        link.head_sha == current_head and snapshot["head_sha"] == link.head_sha and
        snapshot["reviewer"] == link.reviewer and
        is_integer(age) and age > 0 and is_integer(observed) and age <= observed and
        observed <= now and is_integer(count) and count >= 0

    cond do
      not valid -> :unknown
      count >= @inherited_retries -> :exhausted
      true -> age
    end
  end

  defp member_age(_, _, _), do: :unknown

  defp preferred_remainder?(batch, rows) do
    batch.sealed == true and length(rows) > 1 and
      Enum.all?(rows, fn {_, _, admission} ->
        s = admission.snapshot
        count = Map.get(s, "remainder_retries", 0)

        s["source"] == "bors.retry" and
          (s["retry_reason"] in @remainder_reasons or s["verified_remainder"] == true) and
          is_integer(s["parent_candidate_id"]) and s["parent_candidate_id"] > 0 and
          is_integer(count) and count in 0..3
      end)
  end

  defp fallback(batch, now) do
    case batch.last_polled do
      seconds when is_integer(seconds) and seconds >= 0 -> seconds * 1_000_000
      _ -> now
    end
  end

  def order_key(batch, ages) do
    {age, source} = Map.fetch!(ages, batch.id)

    rank =
      case source do
        {:diagnostic_remainder, size} -> {0, -size}
        _ -> {1, 0}
      end

    # Running batches precede waiting even with an explicit priority >100.
    {if(batch.state == :running, do: 0, else: 1), -batch.priority, rank, age, batch.last_polled,
     batch.id}
  end

  def eligible?(batch, now_seconds, poll_due?) do
    batch.state == :waiting and poll_due? and
      (batch.recovery_after || 0) <= now_seconds and
      (batch.promotion_retry_at || 0) <= now_seconds
  end
end
