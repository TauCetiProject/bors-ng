defmodule BorsNG.Worker.QueueApproval do
  @moduledoc "Accepted, exact-head queue intent; recovered inside the existing batcher."
  import Ecto.Query
  alias BorsNG.Database.{Batch, LinkPatchBatch, Patch, Repo}

  def remember(patch, reviewer) do
    approval = %{"head_sha" => patch.commit, "reviewer" => reviewer, "state" => "approved"}
    patch |> Patch.changeset(%{queue_approval: approval, queue_pending: true}) |> Repo.update!()
  end

  def stop(patches, reason) do
    Enum.each(patches, fn patch ->
      approval = %{
        "head_sha" => patch.commit,
        "state" => "withdrawn",
        "reason" => to_string(reason)
      }

      patch
      |> Patch.changeset(%{queue_approval: approval, queue_pending: false})
      |> Repo.update!()
    end)
  end

  def fail(patches, batch, reason) do
    Enum.each(patches, fn patch ->
      approval = %{
        "head_sha" => patch.commit,
        "state" => "failed",
        "batch_id" => batch.id,
        "reason" => to_string(reason)
      }

      patch
      |> Patch.changeset(%{queue_approval: approval, queue_pending: false})
      |> Repo.update!()
    end)
  end

  def blocked?(%Patch{
        queue_approval: %{"head_sha" => head, "state" => state, "reason" => reason},
        commit: head
      }) do
    state == "failed" or (state == "withdrawn" and reason in ["requested", "cancel_all"])
  end

  def blocked?(_), do: false

  def candidates(project_id, after_id, limit) do
    active =
      from(l in LinkPatchBatch,
        join: b in Batch,
        on: b.id == l.batch_id,
        where: b.state in [:waiting, :running],
        select: %{patch_id: l.patch_id, head_sha: l.head_sha}
      )

    q =
      from(p in Patch,
        left_join: a in subquery(active),
        on: a.patch_id == p.id and a.head_sha == p.commit,
        where:
          p.project_id == ^project_id and p.open and not p.is_draft and p.queue_pending and
            is_nil(a.patch_id),
        order_by: p.id
      )

    newer = Repo.all(from(p in q, where: p.id > ^after_id, limit: ^limit))

    older =
      if length(newer) < limit,
        do: Repo.all(from(p in q, where: p.id <= ^after_id, limit: ^(limit - length(newer)))),
        else: []

    newer ++ older
  end
end
