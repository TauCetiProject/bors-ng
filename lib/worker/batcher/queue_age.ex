defmodule BorsNG.Worker.Batcher.QueueAge do
  @moduledoc """
  Pure waiting-group FIFO policy. Only complete, exact current-member admissions
  supply age. Poll timestamps remain operational; see deploy/queue-age.md.
  """

  @inherited_retries 3

  # Old journals have no counter: allow a bounded rollout grace, then persist
  # every new retry. Saturation avoids overflow and cannot renew the preference.
  def next_retry(nil), do: @inherited_retries

  def next_retry(snapshot) do
    case Map.get(snapshot, "queue_age_retries", 0) do
      count when is_integer(count) and count >= 0 -> min(count + 1, @inherited_retries)
      _ -> @inherited_retries
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

      :exhausted in ages or (Map.get(batch, :recovery_failures) || 0) >= @inherited_retries ->
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

    valid =
      admission.project_id == batch.project_id and admission.into_branch == batch.into_branch and
        admission.batch_id == batch.id and link.batch_id == batch.id and
        admission.link_id == link.id and snapshot["base"] == batch.into_branch and
        snapshot["patch_id"] == link.patch_id and is_binary(link.head_sha) and
        link.head_sha == current_head and snapshot["head_sha"] == link.head_sha and
        snapshot["reviewer"] == link.reviewer and
        snapshot["manual_approval"] == link.manual_approval and
        is_integer(age) and age > 0 and is_integer(observed) and age <= observed and
        observed <= now and is_integer(count) and count >= 0

    cond do
      not valid -> :unknown
      count >= @inherited_retries -> :exhausted
      true -> age
    end
  end

  defp member_age(_, _, _), do: :unknown

  defp fallback(batch, now) do
    case batch.last_polled do
      seconds when is_integer(seconds) and seconds >= 0 -> seconds * 1_000_000
      _ -> now
    end
  end

  def order_key(batch, ages) do
    {age, _source} = Map.fetch!(ages, batch.id)
    # Running batches precede waiting even with an explicit priority >100.
    {if(batch.state == :running, do: 0, else: 1), -batch.priority, age, batch.last_polled,
     batch.id}
  end

  def eligible?(batch, now_seconds, poll_due?) do
    batch.state == :waiting and poll_due? and
      (batch.recovery_after || 0) <= now_seconds and
      (batch.promotion_retry_at || 0) <= now_seconds
  end
end
