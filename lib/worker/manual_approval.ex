defmodule BorsNG.Worker.ManualApproval do
  @moduledoc "Exact-revision human approval for TauCeti's path restriction."
  import Ecto.Query
  alias BorsNG.Database.{Batch, LinkPatchBatch, Patch, Project, Repo, User}
  alias BorsNG.GitHub

  @repo "TauCetiProject/TauCeti"

  def scoped?(%Project{name: @repo}, "main"), do: true
  def scoped?(_, _), do: false

  # Actual authenticated command author, never the name in `r=somebody`.
  def authorized?(%Project{name: @repo} = project, %User{user_xref: actor}) do
    conn = Project.installation_connection(project.repo_xref, Repo)
    administrator?(conn, actor) == {:ok, true}
  end

  def authorized?(_, _), do: false

  def valid?(%Patch{into_branch: "main", open: true, is_draft: false} = patch) do
    case patch.manual_approval do
      %{"actor_id" => actor, "head_sha" => head, "merge_base_sha" => base}
      when is_integer(actor) and actor > 0 ->
        head == patch.commit and sha?(head) and sha?(base)

      _ ->
        false
    end
  end

  def valid?(_), do: false

  def grant(conn, project, patch, actor, head, created_at \\ DateTime.utc_now())

  def grant(conn, %Project{name: @repo}, patch, actor, head, created_at) do
    with {:ok, true} <- administrator?(conn, actor),
         true <- fresh_command?(patch, created_at),
         true <- patch.commit == head,
         true <- patch.open and not patch.is_draft and patch.into_branch == "main",
         {:ok, pr} <- GitHub.get_pr(conn, patch.pr_xref),
         true <-
           pr.head_sha == head and pr.state == :open and not pr.draft and pr.base_ref == "main",
         {:ok, base} <- GitHub.get_pr_merge_base(conn, patch.pr_xref, head),
         true <- sha?(head) and sha?(base) do
      {:ok, patch} =
        Repo.transaction(fn ->
          batch_ids =
            Repo.all(
              from(l in LinkPatchBatch,
                where: l.patch_id == ^patch.id and l.head_sha == ^head,
                select: l.batch_id
              )
            )

          # Match construction/recovery lock order: batches, then patch, then
          # links. In particular, avoid a MySQL UPDATE JOIN acquiring a batch
          # lock after the patch write while recovery holds the inverse pair.
          active_ids =
            Repo.all(
              from(b in Batch,
                where: b.id in ^batch_ids and b.state in ^[:waiting, :running],
                order_by: b.id,
                lock: "FOR UPDATE",
                select: b.id
              )
            )

          patch =
            patch
            |> Patch.changeset(%{
              manual_approval: %{
                "actor_id" => actor,
                "head_sha" => head,
                "merge_base_sha" => base
              },
              merge_eligibility: nil
            })
            |> Repo.update!()

          # Publish the patch and its queued memberships atomically. A reader
          # must not mistake a partially persisted takeover for revocation.
          Repo.update_all(
            from(l in LinkPatchBatch,
              where:
                l.patch_id == ^patch.id and l.head_sha == ^head and
                  l.batch_id in ^active_ids
            ),
            set: [manual_approval: patch.manual_approval]
          )

          patch
        end)

      {:ok, patch}
    else
      {:ok, false} -> {:error, :manual_approval_changed}
      false -> {:error, :manual_approval_changed}
      _ -> :waiting
    end
  end

  def grant(_, _, _, _, _, _), do: {:error, :manual_approval_changed}

  # Check the live diff even when a push, retarget or cancellation webhook was missed.
  def preflight(conn, patch) do
    with true <- valid?(patch),
         {:ok, true} <- administrator?(conn, patch.manual_approval["actor_id"]),
         {:ok, pr} <- GitHub.get_pr(conn, patch.pr_xref),
         true <-
           pr.head_sha == patch.commit and pr.state == :open and not pr.draft and
             pr.base_ref == "main",
         {:ok, base} <- GitHub.get_pr_merge_base(conn, patch.pr_xref, patch.commit),
         true <- base == patch.manual_approval["merge_base_sha"] do
      :ok
    else
      {:ok, false} -> {:error, :manual_approval_changed}
      false -> {:error, :manual_approval_changed}
      _ -> :waiting
    end
  end

  defp administrator?(conn, actor) when is_integer(actor) and actor > 0,
    do: GitHub.repository_admin?(conn, actor)

  defp administrator?(_, _), do: {:ok, false}

  def revoke(patch_id) do
    patch = Repo.get!(Patch, patch_id)
    patch |> Patch.changeset(%{manual_approval: nil}) |> Repo.update!()

    if patch.manual_approval do
      project = Repo.get!(Project, patch.project_id)
      conn = Project.installation_connection(project.repo_xref, Repo)
      GitHub.reset_scope(conn, patch.manual_approval["head_sha"])
    end
  end

  defp fresh_command?(patch, value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, timestamp, _} -> fresh_command?(patch, timestamp)
      _ -> false
    end
  end

  defp fresh_command?(%Patch{manual_revision_at: nil}, %DateTime{}), do: true

  defp fresh_command?(patch, %DateTime{} = timestamp),
    do: DateTime.compare(timestamp, patch.manual_revision_at) != :lt

  defp fresh_command?(_, _), do: false

  defp sha?(s), do: is_binary(s) and Regex.match?(~r/\A[0-9a-f]{40}\z/, s)
end
