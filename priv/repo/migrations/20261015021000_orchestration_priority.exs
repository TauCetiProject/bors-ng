defmodule BorsNG.Repo.Migrations.OrchestrationPriority do
  use Ecto.Migration
  import Ecto.Query

  def up do
    alter table(:orchestration_jobs) do
      add(:priority, :integer, null: false, default: 2)
      add(:claimed_at, :bigint)
      add(:completed_at, :bigint)
    end

    alter table(:projects) do
      add(:orchestration_streak, :integer, null: false, default: 0)
    end

    create(index(:orchestration_jobs, [:state, :priority, :available_at, :id]))
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
        BorsNG.Worker.Orchestration.Outbox.schedule_attributes(
          job.project_id,
          job.kind,
          :erlang.binary_to_term(job.payload),
          repo()
        )

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

  def down do
    drop(index(:orchestration_jobs, [:state, :priority, :available_at, :id]))

    alter table(:orchestration_jobs) do
      remove(:priority)
      remove(:claimed_at)
      remove(:completed_at)
    end

    alter(table(:projects), do: remove(:orchestration_streak))
  end
end
