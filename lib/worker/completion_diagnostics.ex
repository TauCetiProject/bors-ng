defmodule BorsNG.Worker.CompletionDiagnostics do
  @moduledoc "Bounded observations of stalled completion, with no scheduling authority."
  import Ecto.Query
  require Logger
  alias BorsNG.Database.{CandidateEvent, Repo, Status}
  alias BorsNG.Worker.CandidateJournal
  alias BorsNG.Worker.Orchestration.Outbox

  def hold(batch, reason) do
    candidate = CandidateJournal.current(batch)

    blockers =
      if reason == :withdrawal_pending do
        ids =
          Repo.all(
            from(l in BorsNG.Database.LinkPatchBatch,
              where: l.batch_id == ^batch.id,
              select: l.patch_id
            )
          )

        Outbox.withdrawal_blockers(batch.project_id, ids)
      else
        %{jobs: [], truncated: false}
      end

    deadline = max(batch.recovery_after || 0, batch.promotion_retry_at || 0)

    record(batch, reason, %{
      next_recheck_at: if(deadline > 0, do: deadline + 1),
      blockers: blockers,
      fallback_poll_at: Outbox.now() + batch.project.batch_poll_period_sec,
      predecessor_id: if(candidate, do: candidate.snapshot["speculative_predecessor_id"])
    })
  rescue
    _ -> :ok
  end

  def record(batch, reason, details \\ %{}) do
    if candidate = CandidateJournal.current(batch) do
      statuses = Repo.all(Status.all_for_batch(batch.id))
      summary = BorsNG.Worker.Batcher.State.summary_database_statuses(statuses)
      # Normal in-progress polling produces no diagnostic traffic.
      if statuses != [] and summary in [:ok, :error] do
        now = Outbox.now()
        reason = Atom.to_string(reason)

        previous =
          Repo.one(
            from(e in CandidateEvent,
              where:
                e.candidate_id == ^candidate.id and e.kind == "completion_deferred" and
                  e.source == "bors.completion" and fragment("?->>'reason'", e.details) == ^reason,
              order_by: [desc: e.id],
              limit: 1
            )
          )

        # Repeated polls coalesce for five minutes, but changed reasons are visible.
        if is_nil(previous) or previous.details["reason"] != reason or
             previous.observed_at <= (now - 300) * 1_000_000 do
          job = Process.get(:orchestration_job)

          payload =
            Map.merge(
              %{
                reason: reason,
                commit: batch.commit,
                base_sha: batch.base_sha,
                summary: summary,
                statuses: Enum.map(statuses, &%{context: &1.identifier, state: &1.state}),
                last_polled: batch.last_polled,
                recovery_after: batch.recovery_after || 0,
                promotion_retry_at: batch.promotion_retry_at || 0,
                fallback_poll_at: batch.last_polled + batch.project.batch_poll_period_sec,
                job_id: if(job, do: job.id),
                attempt: if(job, do: job.attempts),
                proof_authority: false
              },
              details
            )

          CandidateJournal.event(candidate, "completion_deferred", "bors.completion", payload)

          Logger.warning(
            "bors_completion_deferred batch=#{batch.id} candidate=#{candidate.id} reason=#{reason} job=#{if(job, do: job.id)}"
          )
        end
      end
    end

    :ok
  rescue
    error ->
      Logger.warning(
        "bors_completion_diagnostic_unavailable batch=#{batch.id} exception=#{inspect(error.__struct__)}"
      )

      :ok
  end
end
