defmodule BorsNG.Worker.BumpRecovery do
  @moduledoc "Bounded unchanged-head dependency recovery inside the serialized batcher."
  import Ecto.Query
  alias BorsNG.Database.{Batch, LinkPatchBatch, Patch, Repo, User}
  alias BorsNG.GitHub

  @reviewer "tauceti-review-bot[bot]"
  @limit 3

  def branch?(ref) when is_binary(ref),
    do: String.starts_with?(ref, "bump-mathlib/") or ref == "hopscotch/lkg-bump"

  def branch?(_), do: false

  def automatic?(patch) do
    is_integer(patch.author_id) and branch?(patch.head_ref) and is_nil(patch.manual_approval) and
      is_nil(patch.bundle_id) and
      match?(%User{login: @reviewer}, Repo.get(User, patch.author_id))
  end

  def plan(conn, project, pr, patch, decision) do
    metadata =
      if get_in(patch.merge_eligibility || %{}, ["head_sha"]) == patch.commit,
        do: patch.merge_eligibility,
        else: %{}

    count = Map.get(metadata, "bump_retry_count", 0)

    with true <- decision["eligible"] and decision["single"] and automatic?(patch),
         true <- pr.head_repo_id == project.repo_xref and pr.base_repo_id == project.repo_xref,
         true <- not patch.queue_pending and is_nil(patch.bundle_reviewer),
         true <- is_integer(count) and count >= 0 and count < @limit,
         %{batch: batch, reviewer: @reviewer} <- latest(patch),
         true <-
           batch.state == :error and permitted_stop?(patch.queue_approval, patch.commit, batch.id),
         1 <-
           Repo.aggregate(from(l in LinkPatchBatch, where: l.batch_id == ^batch.id), :count, :id),
         false <- active?(patch),
         {:ok, failed_base} <- failed_base(conn, batch),
         {:ok, %{commit: current}} <- GitHub.get_recovery_base(conn),
         true <-
           sha?(current) and current != failed_base and current != metadata["bump_retry_base"],
         {:ok, :ahead} <- GitHub.compare_status(conn, failed_base, current) do
      %{
        "bump_retry_count" => count + 1,
        "bump_retry_base" => current,
        "bump_retry_batch" => batch.id
      }
    else
      _ -> nil
    end
  end

  def record(decision, patch, retry) do
    previous =
      if get_in(patch.merge_eligibility || %{}, ["head_sha"]) == patch.commit,
        do:
          Map.take(
            patch.merge_eligibility,
            ~w(bump_retry_count bump_retry_base bump_retry_batch bump_repair_batch)
          ),
        else: %{}

    decision |> Map.merge(previous) |> Map.merge(retry || %{})
  end

  defp permitted_stop?(nil, _, _), do: true

  defp permitted_stop?(
         %{"state" => "failed", "reason" => "build", "head_sha" => head, "batch_id" => batch},
         head,
         batch
       ),
       do: true

  defp permitted_stop?(_, _, _), do: false

  defp latest(patch) do
    Repo.one(
      from(l in LinkPatchBatch,
        join: b in Batch,
        on: b.id == l.batch_id,
        where:
          l.patch_id == ^patch.id and l.head_sha == ^patch.commit and
            b.project_id == ^patch.project_id and b.into_branch == "main",
        order_by: [desc: b.id],
        limit: 1,
        select: %{batch: b, reviewer: l.reviewer}
      )
    )
  end

  defp active?(patch) do
    Repo.exists?(
      from(l in LinkPatchBatch,
        join: b in Batch,
        on: b.id == l.batch_id,
        where:
          l.patch_id == ^patch.id and l.head_sha == ^patch.commit and
            b.state in [:waiting, :running]
      )
    )
  end

  defp failed_base(conn, %{base_sha: nil, commit: staged}) do
    if sha?(staged), do: GitHub.get_staging_base(conn, staged), else: {:error, :unknown_base}
  end

  defp failed_base(_, %{base_sha: base}) do
    if sha?(base), do: {:ok, base}, else: {:error, :unknown_base}
  end

  defp sha?(sha), do: is_binary(sha) and Regex.match?(~r/\A[0-9a-f]{40}\z/, sha)

  def report_failure(conn, batch, [patch]) do
    if automatic?(patch) do
      case GitHub.post_bump_failure(conn, patch.commit, batch.id) do
        :ok ->
          patch = Repo.get!(Patch, patch.id)

          if is_map(patch.merge_eligibility) do
            metadata = Map.put(patch.merge_eligibility, "bump_repair_batch", batch.id)
            patch |> Patch.changeset(%{merge_eligibility: metadata}) |> Repo.update!()
          end

          :ok

        error ->
          error
      end
    end
  end

  def report_failure(_, _, _), do: :ok

  def ensure_repair_signal(conn, patch) do
    with false <- patch.queue_pending,
         true <- is_nil(patch.bundle_reviewer),
         %{"state" => "failed", "reason" => "build", "head_sha" => head, "batch_id" => id}
         when head == patch.commit <- patch.queue_approval,
         true <- get_in(patch.merge_eligibility || %{}, ["bump_repair_batch"]) != id,
         %{batch: %{id: ^id, state: :error} = batch} <- latest(patch),
         1 <- Repo.aggregate(from(l in LinkPatchBatch, where: l.batch_id == ^id), :count, :id) do
      report_failure(conn, batch, [patch])
    else
      _ -> :ok
    end
  end

  defp confirmed_singleton?(patch, id) do
    match?(%{batch: %{id: ^id, state: :error}}, latest(patch)) and
      Repo.aggregate(from(l in LinkPatchBatch, where: l.batch_id == ^id), :count, :id) == 1
  end

  def repair_candidates(project_id, base) do
    query =
      from(p in Patch,
        where:
          p.project_id == ^project_id and p.open and not p.is_draft and
            not p.queue_pending and is_nil(p.bundle_reviewer) and is_nil(p.bundle_id) and
            is_nil(p.manual_approval) and
            (like(p.head_ref, "bump-mathlib/%") or p.head_ref == "hopscotch/lkg-bump")
      )

    query = if base, do: from(p in query, where: p.into_branch == ^base), else: query

    Repo.all(query)
    |> Enum.flat_map(fn patch ->
      case patch.queue_approval do
        %{"head_sha" => head, "state" => "failed", "reason" => "build", "batch_id" => id}
        when head == patch.commit ->
          if automatic?(patch) and not active?(patch) and confirmed_singleton?(patch, id),
            do: [%{pr: patch.pr_xref, head_sha: head, batch_id: id}],
            else: []

        _ ->
          []
      end
    end)
  end
end
