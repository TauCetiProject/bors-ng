defmodule BorsNG.Repo.Migrations.CandidateJournal do
  use Ecto.Migration

  # No backfill: historical timestamps, policy and ancestry are unknown.
  # No cascading foreign keys: withdrawal/deletion must not erase evidence.
  def up do
    create_if_not_exists table(:candidate_admissions) do
      add(:project_id, :bigint, null: false)
      add(:into_branch, :string, null: false)
      add(:batch_id, :bigint, null: false)
      add(:link_id, :bigint, null: false)
      add(:snapshot, :map, null: false)
      add(:observed_at, :bigint, null: false)
    end

    create_if_not_exists table(:candidates) do
      add(:project_id, :bigint, null: false)
      add(:into_branch, :string, null: false)
      add(:batch_id, :bigint, null: false)
      add(:generation, :integer, null: false)
      add(:snapshot, :map, null: false)
      add(:observed_at, :bigint, null: false)
    end

    create_if_not_exists table(:candidate_events) do
      add(:candidate_id, :bigint, null: false)
      add(:kind, :string, null: false)
      add(:source, :string, null: false)
      add(:details, :map, null: false)
      add(:observed_at, :bigint, null: false)
    end

    flush()

    ensure_index(unique_index(:candidate_admissions, [:link_id]))
    ensure_index(index(:candidate_admissions, [:project_id, :batch_id]))
    ensure_index(unique_index(:candidates, [:batch_id, :generation]))
    ensure_index(index(:candidates, [:project_id, :id]))
    ensure_index(index(:candidate_events, [:candidate_id, :id]))
  end

  def down do
    drop_if_exists(table(:candidate_events))
    drop_if_exists(table(:candidates))
    drop_if_exists(table(:candidate_admissions))
  end

  defp ensure_index(index) do
    if repo().__adapter__() == Ecto.Adapters.MyXQL do
      %{rows: [[count]]} =
        Ecto.Adapters.SQL.query!(
          repo(),
          """
          SELECT COUNT(*) FROM information_schema.statistics
          WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?
          """,
          [to_string(index.table), to_string(index.name)]
        )

      if count == 0, do: create(index)
    else
      create_if_not_exists(index)
    end
  end
end
