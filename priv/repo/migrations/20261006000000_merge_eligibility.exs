defmodule BorsNG.Repo.Migrations.MergeEligibility do
  use Ecto.Migration

  def change do
    alter table(:patches) do
      add(:merge_eligibility_id, :bigint)
      add(:merge_eligibility, :map)
    end
  end
end
