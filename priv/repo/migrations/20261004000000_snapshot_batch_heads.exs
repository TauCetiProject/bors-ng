defmodule BorsNG.Repo.Migrations.SnapshotBatchHeads do
  use Ecto.Migration

  def change do
    alter table(:link_patch_batch) do
      add(:head_sha, :string)
    end
  end
end
