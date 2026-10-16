defmodule BorsNG.Repo.Migrations.QueueMembershipIndex do
  use Ecto.Migration

  # PostgreSQL does not index foreign keys automatically. The existing indexes
  # lead with patch_id, not batch_id; scheduling must seek current membership.
  # MySQL already creates a supporting FK index.
  def up do
    if repo().__adapter__() != Ecto.Adapters.MyXQL do
      create_if_not_exists(index(:link_patch_batch, [:batch_id]))
    end
  end

  def down do
    if repo().__adapter__() != Ecto.Adapters.MyXQL do
      drop_if_exists(index(:link_patch_batch, [:batch_id]))
    end
  end
end
