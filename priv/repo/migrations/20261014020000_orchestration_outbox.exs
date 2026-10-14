defmodule BorsNG.Repo.Migrations.OrchestrationOutbox do
  use Ecto.Migration

  def up do
    # Immutable batch recipes can exceed MySQL's 64 KiB BLOB limit.
    payload_type = if repo().__adapter__() == Ecto.Adapters.MyXQL, do: :mediumblob, else: :binary

    create_if_not_exists table(:orchestration_jobs) do
      add(:project_id, :bigint, null: false)
      add(:kind, :string, null: false)
      add(:subject_id, :bigint)
      add(:payload, payload_type, null: false)
      add(:dedup_key, :string)
      add(:state, :string, null: false, default: "pending")
      add(:attempts, :integer, null: false, default: 0)
      add(:available_at, :bigint, null: false)
      add(:lease_until, :bigint, null: false, default: 0)
      add(:token, :string)
      add(:last_error, :string)
      timestamps()
    end

    flush()
    ensure_approval_token()
    flush()
    ensure_index(unique_index(:orchestration_jobs, [:dedup_key]))
    ensure_index(index(:orchestration_jobs, [:state, :available_at, :id]))
    ensure_index(index(:orchestration_jobs, [:project_id, :state]))
  end

  def down do
    alter(table(:patches), do: remove(:orchestration_approval_token))
    drop_if_exists(table(:orchestration_jobs))
  end

  defp ensure_approval_token do
    sql =
      if repo().__adapter__() == Ecto.Adapters.MyXQL,
        do:
          "SELECT COUNT(*) FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'patches' AND COLUMN_NAME = 'orchestration_approval_token'",
        else:
          "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'patches' AND column_name = 'orchestration_approval_token'"

    %{rows: [[count]]} = Ecto.Adapters.SQL.query!(repo(), sql, [])

    if count == 0 do
      alter(table(:patches), do: add(:orchestration_approval_token, :string))
    end
  end

  defp ensure_index(index) do
    if repo().__adapter__() == Ecto.Adapters.MyXQL do
      %{rows: [[count]]} =
        Ecto.Adapters.SQL.query!(
          repo(),
          "SELECT COUNT(*) FROM information_schema.statistics WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
          [to_string(index.table), to_string(index.name)]
        )

      if count == 0, do: create(index)
    else
      create_if_not_exists(index)
    end
  end
end
