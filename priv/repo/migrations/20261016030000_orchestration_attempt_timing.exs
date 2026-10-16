defmodule BorsNG.Repo.Migrations.OrchestrationAttemptTiming do
  use Ecto.Migration

  def change do
    alter table(:orchestration_jobs) do
      add(:attempt_timing, :map)
    end
  end
end
