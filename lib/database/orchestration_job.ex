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
    field(:wake_version, :integer, default: 0)
    field(:claimed_version, :integer)
    field(:rerun_enqueued_at_ms, :integer)
    field(:rerun_at, :integer)
    field(:priority, :integer, default: 2)
    field(:enqueued_at_ms, :integer)
    field(:claimed_at_ms, :integer)
    field(:completed_at_ms, :integer)
    field(:attempts, :integer, default: 0)
    field(:available_at, :integer)
    field(:lease_until, :integer, default: 0)
    field(:token, :string)
    field(:last_error, :string)
    timestamps()
  end
end
