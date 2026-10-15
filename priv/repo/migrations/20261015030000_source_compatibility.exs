defmodule BorsNG.Repo.Migrations.SourceCompatibility do
  use Ecto.Migration

  def change do
    alter(table(:projects), do: add(:source_compatibility, :map))
    alter(table(:patches), do: add(:source_compatibility, :map))
  end
end
