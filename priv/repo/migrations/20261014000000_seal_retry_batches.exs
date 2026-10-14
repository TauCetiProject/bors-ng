defmodule BorsNG.Repo.Migrations.SealRetryBatches do
  use Ecto.Migration
  import Ecto.Query

  def up do
    if repo().__adapter__() == Ecto.Adapters.MyXQL do
      # MySQL 8 lacks ADD COLUMN IF NOT EXISTS, and DDL is not transactional.
      # Inspect the column so an interrupted migration can restart safely.
      unless mysql_sealed_column_exists?() do
        alter table(:batches) do
          add(:sealed, :boolean, default: false, null: false)
        end
      end
    else
      alter table(:batches) do
        add_if_not_exists(:sealed, :boolean, default: false, null: false)
      end
    end

    flush()

    # Historical retries have no durable lineage. Conservatively close all
    # existing active TauCeti/main batches to admission, including running
    # batches that may be paused back to waiting. Do not touch membership,
    # priorities, timestamps, or approval snapshots.
    projects = from(p in "projects", where: p.name == "TauCetiProject/TauCeti", select: p.id)

    repo().update_all(
      from(b in "batches",
        where:
          b.project_id in subquery(projects) and b.into_branch == "main" and b.state in [0, 1]
      ),
      set: [sealed: true]
    )
  end

  def down do
    if repo().__adapter__() == Ecto.Adapters.MyXQL do
      if mysql_sealed_column_exists?() do
        alter table(:batches) do
          remove(:sealed, :boolean)
        end
      end
    else
      alter table(:batches) do
        remove_if_exists(:sealed, :boolean)
      end
    end
  end

  defp mysql_sealed_column_exists? do
    %{rows: [[count]]} =
      Ecto.Adapters.SQL.query!(repo(), """
      SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'batches' AND column_name = 'sealed'
      """)

    count > 0
  end
end
