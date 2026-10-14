defmodule BorsNG.Database.Candidate do
  @moduledoc "Immutable assembly input; lifecycle facts live in CandidateEvent."
  use Ecto.Schema
  @type t :: %__MODULE__{}

  schema "candidates" do
    field(:project_id, :integer)
    field(:into_branch, :string)
    field(:batch_id, :integer)
    field(:generation, :integer)
    field(:snapshot, :map)
    field(:observed_at, :integer)
  end
end

defmodule BorsNG.Database.CandidateAdmission do
  @moduledoc "Immutable member snapshot, retained after queue links are deleted."
  use Ecto.Schema

  schema "candidate_admissions" do
    field(:project_id, :integer)
    field(:into_branch, :string)
    field(:batch_id, :integer)
    field(:link_id, :integer)
    field(:snapshot, :map)
    field(:observed_at, :integer)
  end
end

defmodule BorsNG.Database.CandidateEvent do
  @moduledoc "Append-only lifecycle observation with source and receipt time."
  use Ecto.Schema

  schema "candidate_events" do
    field(:candidate_id, :integer)
    field(:kind, :string)
    field(:source, :string)
    field(:details, :map)
    field(:observed_at, :integer)
  end
end
