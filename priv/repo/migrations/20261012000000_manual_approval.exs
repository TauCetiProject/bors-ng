defmodule BorsNG.Repo.Migrations.ManualApproval do
  use Ecto.Migration

  def change do
    alter table(:patches) do
      add(:manual_approval, :map)
      add(:manual_revision_at, :utc_datetime_usec)
    end

    alter table(:link_patch_batch) do
      add(:manual_approval, :map)
    end

    alter table(:batches) do
      add(:base_sha, :string)
    end
  end
end
