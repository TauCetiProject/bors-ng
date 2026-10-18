defmodule BorsNG.Repo.Migrations.PreflightNotice do
  use Ecto.Migration

  def change do
    alter(table(:patches), do: add(:preflight_notice, :string))
  end
end
