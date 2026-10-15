defmodule BorsNG.Repo.Migrations.OrchestrationWakeVersions do
  use Ecto.Migration

  def change do
    alter table(:orchestration_jobs) do
      add(:wake_version, :bigint, null: false, default: 0)
      add(:claimed_version, :bigint)
      add(:rerun_enqueued_at_ms, :bigint)
      add(:rerun_at, :bigint)
    end
  end
end
