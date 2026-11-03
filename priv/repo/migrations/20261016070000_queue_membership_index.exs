defmodule BorsNG.Repo.Migrations.QueueMembershipIndex do
  use Ecto.Migration

  # PostgreSQL does not index foreign keys automatically. The existing indexes
  # lead with patch_id, not batch_id; scheduling must seek current membership.
  def up do
    create_if_not_exists(index(:link_patch_batch, [:batch_id]))
  end

  def down do
    drop_if_exists(index(:link_patch_batch, [:batch_id]))
  end
end
