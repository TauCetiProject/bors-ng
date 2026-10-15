defmodule BorsNG.Database.OrchestrationJob do
  @moduledoc "Durable scheduler mailbox. Payloads are internal Erlang terms, never client input."
  use Ecto.Schema

  schema "orchestration_jobs" do
    field(:project_id, :integer)
    field(:kind, :string)
    field(:subject_id, :integer)
    field(:payload, :binary)
    field(:dedup_key, :string)
    field(:state, :string, default: "pending")
    field(:priority, :integer, default: 2)
    field(:claimed_at, :integer)
    field(:completed_at, :integer)
    field(:attempts, :integer, default: 0)
    field(:available_at, :integer)
    field(:lease_until, :integer, default: 0)
    field(:token, :string)
    field(:last_error, :string)
    timestamps()
  end
end
