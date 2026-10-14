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

  # A queue priority below the running-batch sentinel (100) cannot preempt CI.
  # Reconcile existing memberships too; deploying policy needs no PR command.
  @queue_priority 50
  def prioritize_waiting(%{name: "TauCetiProject/TauCeti", id: project_id}) do
    candidates =
      Repo.all(
        from(b in Batch,
          join: l in LinkPatchBatch,
          on: l.batch_id == b.id,
          join: p in Patch,
          on: p.id == l.patch_id,
          join: u in User,
          on: u.id == p.author_id,
          where:
            b.project_id == ^project_id and b.into_branch == "main" and b.state == :waiting and
              p.project_id == ^project_id and p.into_branch == "main" and p.open and
              not p.is_draft and
              p.is_single and is_nil(p.bundle_id) and is_nil(p.manual_approval) and
              is_nil(l.manual_approval) and l.head_sha == p.commit and l.reviewer == ^@reviewer and
              u.login == ^@reviewer and
              (like(p.head_ref, "bump-mathlib/%") or p.head_ref == "hopscotch/lkg-bump") and
              (b.priority < ^@queue_priority and p.priority == 0),
          select: {b, p}
        )
      )

    Enum.each(candidates, fn {batch, patch} ->
      proof = patch.merge_eligibility || %{}

      if proof["head_sha"] == patch.commit and proof["eligible"] == true and
           proof["review_safe"] == true and proof["single"] == true and
           Repo.aggregate(from(l in LinkPatchBatch, where: l.batch_id == ^batch.id), :count, :id) ==
             1 and
           match?(
             %{"state" => "approved", "head_sha" => head, "reviewer" => @reviewer}
             when head == patch.commit,
             patch.queue_approval
           ) do
        # Deliberately avoid the manual priority command's preemption side effect.
        if batch.priority < @queue_priority,
          do: batch |> Batch.changeset(%{priority: @queue_priority}) |> Repo.update!()
      end
    end)

    :ok
  end

  def prioritize_waiting(_), do: :ok

  def plan(conn, project, pr, patch, decision) do
    metadata =
      if get_in(patch.merge_eligibility || %{}, ["head_sha"]) == patch.commit,
        do: patch.merge_eligibility,
        else: %{}

    count = Map.get(metadata, "bump_retry_count", 0)

    with true <- decision["eligible"] and decision["single"] and automatic?(patch),
         true <- pr.head_repo_id == project.repo_xref and pr.base_repo_id == project.repo_xref,
         true <- not patch.queue_pending and is_nil(patch.bundle_reviewer),
         true <- is_integer(count) and count >= 0,
         %{batch: batch, reviewer: @reviewer} <- latest(patch),
         true <-
           batch.state == :error and permitted_stop?(patch.queue_approval, patch.commit, batch.id),
         1 <-
           Repo.aggregate(from(l in LinkPatchBatch, where: l.batch_id == ^batch.id), :count, :id),
         false <- active?(patch) do
      cond do
        get_in(patch.queue_approval || %{}, ["repair_batch"]) == batch.id ->
          {:repair, batch.id}

        count >= @limit ->
          {:repair, batch.id}

        true ->
          with {:ok, failed_base} <- failed_base(conn, batch),
               {:ok, %{commit: current}} <- GitHub.get_recovery_base(conn),
               true <- sha?(current) do
            if current == failed_base do
              {:repair, batch.id}
            else
              case GitHub.compare_status(conn, failed_base, current) do
                {:ok, :ahead} ->
                  if current != metadata["bump_retry_base"],
                    do:
                      {:retry,
                       %{
                         "bump_retry_count" => count + 1,
                         "bump_retry_base" => current,
                         "bump_retry_batch" => batch.id
                       }},
                    else: :defer

                {:ok, status} when status in [:identical, :behind, :diverged] ->
                  {:repair, batch.id}

                _ ->
                  :defer
              end
            end
          else
            _ -> :defer
          end
      end
    else
      _ -> :defer
    end
  end

  def record(decision, patch, retry) do
    previous =
      if get_in(patch.merge_eligibility || %{}, ["head_sha"]) == patch.commit,
        do:
          Map.take(
            patch.merge_eligibility,
            ~w(bump_retry_count bump_retry_base bump_retry_batch failure_retry_count failure_retry_base failure_retry_candidate)
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

  # Persist exclusive repair ownership before publishing its advisory status.
  # A later main advance must never retry a head already handed to an editor.
  def handoff(conn, patch, batch_id) do
    approval =
      (patch.queue_approval ||
         %{
           "head_sha" => patch.commit,
           "state" => "failed",
           "reason" => "build",
           "batch_id" => batch_id
         })
      |> Map.put("repair_batch", batch_id)

    patch = patch |> Patch.changeset(%{queue_approval: approval}) |> Repo.update!()

    if approval["repair_published"] != true do
      case GitHub.post_bump_failure(conn, patch.commit, batch_id) do
        :ok ->
          patch
          |> Patch.changeset(%{queue_approval: Map.put(approval, "repair_published", true)})
          |> Repo.update!()

        _ ->
          :ok
      end
    end

    :ok
  end

  def restore_failed_preflight(patch, reason) when reason in [:preflight, :preflight_timeout] do
    metadata = patch.merge_eligibility || %{}
    batch_id = metadata["bump_retry_batch"]
    count = metadata["bump_retry_count"]

    with true <- automatic?(patch),
         true <- metadata["head_sha"] == patch.commit,
         true <- is_integer(count) and count > 0,
         %{"state" => "approved", "head_sha" => head, "reviewer" => @reviewer}
         when head == patch.commit <-
           patch.queue_approval,
         true <- confirmed_singleton?(patch, batch_id),
         false <- active?(patch) do
      metadata =
        metadata
        |> Map.put(
          "bump_retry_count",
          if(reason == :preflight_timeout, do: count - 1, else: count)
        )
        |> Map.drop(["bump_retry_batch"])

      patch
      |> Patch.changeset(%{
        merge_eligibility: metadata,
        queue_pending: false,
        bundle_reviewer: nil,
        queue_approval: %{
          "head_sha" => patch.commit,
          "state" => "failed",
          "reason" => "build",
          "batch_id" => batch_id
        }
      })
      |> Repo.update!()

      true
    else
      _ -> false
    end
  end

  def restore_failed_preflight(_, _), do: false

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
        when head == patch.commit and is_integer(id) and id > 0 ->
          if patch.queue_approval["repair_batch"] == id and automatic?(patch) and
               not active?(patch) and confirmed_singleton?(patch, id),
             do: [%{pr: patch.pr_xref, head_sha: head, batch_id: id}],
             else: []

        _ ->
          []
      end
    end)
  end
end
