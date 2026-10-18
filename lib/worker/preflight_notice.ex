defmodule BorsNG.Worker.PreflightNotice do
  @moduledoc "The latest public preflight rejection, independent of scheduler job identity."
  require Logger
  import Ecto.Query
  alias BorsNG.Database.{Patch, Project, Repo}
  alias BorsNG.Worker.Batcher.Message
  alias BorsNG.Worker.Orchestration.Outbox

  def reject(conn, patch, reason) do
    fingerprint =
      {patch.commit, patch.into_branch, reason}
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    body = Message.generate_message({:preflight, reason})

    if Process.get(:orchestration_executor, false) do
      # Same lock order as outbox producers/claims. The observation and durable
      # delivery intent commit together; a crash cannot record an unsent notice.
      {:ok, :ok} =
        Repo.transaction(fn ->
          Repo.one!(from(p in Project, where: p.id == ^patch.project_id, lock: "FOR UPDATE"))
          current = Repo.one!(from(p in Patch, where: p.id == ^patch.id, lock: "FOR UPDATE"))

          if new_rejection?(current, patch, fingerprint) do
            # A fresh transition needs a fresh marker, including A -> B -> A.
            key = "preflight:#{patch.id}:" <> Base.encode16(:crypto.strong_rand_bytes(16))
            marker = "<!-- bors-outbox #{key} -->"

            Outbox.enqueue(
              patch.project_id,
              "notice",
              {conn, patch.pr_xref, body <> "\n\n" <> marker, marker},
              key
            )

            remember(current, fingerprint)
          end

          :ok
        end)

      :ok
    else
      # Legacy delivery keeps its existing synchronous error handling. Record
      # only acknowledged writes, and never hold a database lock during HTTP.
      current = Repo.get!(Patch, patch.id)

      if new_rejection?(current, patch, fingerprint) do
        case BorsNG.GitHub.post_comment(conn, patch.pr_xref, body) do
          :ok ->
            remember(current, fingerprint)
            :ok

          error ->
            Logger.warning(
              "preflight_notice: failed to post comment for patch #{patch.id}: #{inspect(error)}"
            )

            error
        end
      else
        :ok
      end
    end
  end

  def recovered(patch) do
    # Unknown/unavailable observations must not rearm a rejection. Call only
    # after a conclusive preflight success or a known pending required status.
    Repo.update_all(
      from(p in Patch,
        where:
          p.id == ^patch.id and p.commit == ^patch.commit and
            p.into_branch == ^patch.into_branch and not is_nil(p.preflight_notice)
      ),
      set: [preflight_notice: nil]
    )

    :ok
  end

  defp new_rejection?(current, patch, fingerprint),
    do:
      current.commit == patch.commit and current.into_branch == patch.into_branch and
        current.preflight_notice != fingerprint

  defp remember(patch, fingerprint),
    do: patch |> Ecto.Changeset.change(preflight_notice: fingerprint) |> Repo.update!()
end
