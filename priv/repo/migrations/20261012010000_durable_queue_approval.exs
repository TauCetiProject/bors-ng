defmodule BorsNG.Database.Repo.Migrations.DurableQueueApproval do
  use Ecto.Migration
  import Ecto.Query

  def up do
    alter table(:patches) do
      add(:queue_approval, :map)
      add(:queue_pending, :boolean, default: false, null: false)
    end

    create(index(:patches, [:project_id, :queue_pending, :id]))
    create(index(:link_patch_batch, [:patch_id, :head_sha, :batch_id]))
    flush()

    # Preserve current accepted memberships. Historical failures and withdrawals
    # are deliberately not interpreted as fresh approvals during the migration.
    repo().all(
      from(p in "patches",
        join: l in "link_patch_batch",
        on: l.patch_id == p.id,
        join: b in "batches",
        on: b.id == l.batch_id,
        where: p.open and not p.is_draft and l.head_sha == p.commit and b.state in [0, 1],
        select: {p.id, l.head_sha, l.reviewer}
      )
    )
    |> Enum.each(fn {id, head, reviewer} ->
      approval = %{"head_sha" => head, "reviewer" => reviewer, "state" => "approved"}

      repo().update_all(from(p in BorsNG.Database.Patch, where: p.id == ^id),
        set: [queue_approval: approval, queue_pending: true]
      )
    end)
  end

  def down do
    drop(index(:patches, [:project_id, :queue_pending, :id]))
    drop(index(:link_patch_batch, [:patch_id, :head_sha, :batch_id]))

    alter table(:patches) do
      remove(:queue_pending)
      remove(:queue_approval)
    end
  end
end
