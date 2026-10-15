defmodule BorsNG.Repo.Migrations.OrchestrationPriority do
  use Ecto.Migration
  import Ecto.Query

  def up do
    alter table(:orchestration_jobs) do
      add_if_not_exists(:priority, :integer, null: false, default: 2)
      add_if_not_exists(:enqueued_at_ms, :bigint)
      add_if_not_exists(:claimed_at_ms, :bigint)
      add_if_not_exists(:completed_at_ms, :bigint)
    end

    alter table(:projects) do
      add_if_not_exists(:orchestration_streak, :integer, null: false, default: 0)
    end

    create_if_not_exists(index(:orchestration_jobs, [:state, :priority, :available_at, :id]))
    flush()
    backfill(0)
  end

  defp backfill(cursor) do
    jobs =
      repo().all(
        from(j in "orchestration_jobs",
          where: j.id > ^cursor and j.state in ["pending", "running"],
          order_by: j.id,
          limit: 128,
          select: %{id: j.id, project_id: j.project_id, kind: j.kind, payload: j.payload}
        )
      )

    Enum.each(jobs, fn job ->
      attributes =
        classify(job, :erlang.binary_to_term(job.payload))

      # Old cancel rows already carry their subject. Other command subjects
      # are needed to fence mutations even during infrastructure backoff.
      changes = [priority: attributes.priority]

      changes =
        if attributes.subject_id,
          do: [{:subject_id, attributes.subject_id} | changes],
          else: changes

      repo().update_all(from(j in "orchestration_jobs", where: j.id == ^job.id), set: changes)
    end)

    if jobs != [], do: backfill(List.last(jobs).id)
  end

  # Freeze the upgrade policy here: future application/schema changes must
  # not change old migrations. Terms are trusted internal database payloads;
  # no functions in them are invoked. A malformed row aborts rather than drops
  # a durable command. Idempotent DDL permits a MySQL upgrade retry after repair.
  defp classify(job, payload) do
    project_id = job.project_id

    subject =
      case {job.kind, payload} do
        {"cast", {:eligibility, pr}} ->
          repo().one(
            from(p in "patches",
              where: p.project_id == ^project_id and p.pr_xref == ^pr,
              select: p.id
            )
          ) || 0

        {"cast", command} when is_tuple(command) and tuple_size(command) >= 2 ->
          if is_integer(elem(command, 1)), do: elem(command, 1)

        _ ->
          nil
      end

    priority =
      case {job.kind, payload} do
        {"dispatch", _} ->
          1

        {kind, _} when kind in ["cancel", "cancel_all"] ->
          0

        {"cast", {:status, {commit, _, _, _}}} ->
          if repo().exists?(
               from(b in "batches", where: b.project_id == ^project_id and b.commit == ^commit)
             ),
             do: 1,
             else: 2

        {"cast", _} ->
          if subject == 0, do: 0, else: 2

        {"info", {:poll, _}} ->
          1

        {"info", {:retry_promotion, _, _}} ->
          1

        {"info", :restore_rechecks} ->
          1

        _ ->
          2
      end

    %{priority: priority, subject_id: subject}
  end

  def down do
    drop(index(:orchestration_jobs, [:state, :priority, :available_at, :id]))

    alter table(:orchestration_jobs) do
      remove(:priority)
      remove(:enqueued_at_ms)
      remove(:claimed_at_ms)
      remove(:completed_at_ms)
    end

    alter(table(:projects), do: remove(:orchestration_streak))
  end
end
