defmodule BorsNG.Repo.Migrations.EligibilityCursor do
  use Ecto.Migration

  def change do
    alter(table(:projects), do: add(:eligibility_cursor, :bigint, default: 0, null: false))
  end
end
