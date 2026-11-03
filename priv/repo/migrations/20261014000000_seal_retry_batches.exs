defmodule BorsNG.Repo.Migrations.SealRetryBatches do
  use Ecto.Migration
  import Ecto.Query

  def up do
    alter table(:batches) do
      add_if_not_exists(:sealed, :boolean, default: false, null: false)
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
    alter table(:batches) do
      remove_if_exists(:sealed, :boolean)
    end
  end
end
