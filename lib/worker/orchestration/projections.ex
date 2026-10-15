defmodule BorsNG.Worker.Orchestration.Projections do
  @moduledoc "Bounded advisory writes, with no staging/main or approval authority."
  import Ecto.Query
  alias BorsNG.Database.{Batch, Repo}
  alias BorsNG.Worker.CandidateJournal
  alias BorsNG.Worker.Orchestration.Outbox

  def enabled?,
    do:
      Process.get(:orchestration_executor, false) and not is_nil(Process.get(:orchestration_job))

  def enqueue(project_id, payload) do
    job = Process.get(:orchestration_job)
    if project_id != job.project_id, do: throw(:projection_project_mismatch)
    key = "projection:#{job.id}:" <> CandidateJournal.digest(:erlang.term_to_binary(payload))
    Outbox.enqueue(project_id, "projection", payload, key)
  end

  def deliver({:status, conn, {sha, status, _message, _url} = data, batch_id}) do
    job = Process.get(:orchestration_job)

    if Repo.exists?(from(b in Batch, where: b.project_id == ^job.project_id and b.commit == ^sha)) do
      # A queued PR advisory can become the tested identity before delivery.
      # The synchronous tested-commit status alone owns that identity.
      :ok
    else
      deliver_status(conn, data, status, batch_id)
    end
  end

  defp deliver_status(conn, data, status, batch_id) do
    case Repo.get(Batch, batch_id) do
      nil when status == :ok ->
        :ok

      nil ->
        :ok = BorsNG.GitHub.post_commit_status_once(conn, data)

      %Batch{state: terminal} when status == :ok and terminal in [:canceled, :error, :conflict] ->
        :ok

      %Batch{state: :ok} when status != :ok ->
        :ok

      batch ->
        # A superseded/unbound generation cannot publish a delayed green.
        if status == :ok and CandidateJournal.latest(batch.id) &&
             is_nil(CandidateJournal.current(batch)),
           do: :ok,
           else: :ok = BorsNG.GitHub.post_commit_status_once(conn, data)
    end
  end
end
