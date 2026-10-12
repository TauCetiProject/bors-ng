defmodule BorsNG.Worker.Batcher.Divider do
  import Ecto.Query
  alias BorsNG.Database.Repo
  alias BorsNG.Database.Batch
  alias BorsNG.Database.Patch
  alias BorsNG.Database.Project
  alias BorsNG.Database.LinkPatchBatch
  alias BorsNG.GitHub

  # Splitting operates on units, not individual patches. A linked bundle
  # (patches sharing a `bundle_id`) is one unit and is never divided. Bisection
  # and conflict isolation cannot separate patches that must merge together.
  # An unbundled patch is a unit of its own.

  def split_batch(patch_links, %Batch{project: project, into_branch: into}) do
    units = group_units(patch_links)

    if Enum.count(units) > 1 do
      {single_units, divisible_units} =
        Enum.split_with(units, &unit_is_single?/1)

      Enum.each(single_units, &clone_batch(&1, project.id, into))

      case divisible_units do
        [] -> :ok
        [unit] -> clone_batch(unit, project.id, into)
        units -> bisect(units, project.id, into)
      end

      :retrying
    else
      :failed
    end
  end

  def split_batch_with_conflicts(patch_links, %Batch{project: project, into_branch: into}) do
    repo_conn = get_repo_conn(project)
    units = group_units(patch_links)

    # A lone bundle unit that still failed is a member-versus-member conflict.
    # The bundle is indivisible, and GitHub reports each member mergeable
    # against the base (not pairwise), so retrying re-conflicts forever. Fail
    # it terminally rather than re-clone the same unit. A lone patch is left
    # to the mergeability logic below: an unknown flag is worth a retry.
    if single_bundle_unit?(units) do
      :failed
    else
      # Mergeable 0, unmergeable 1: fail, no retry.
      # Mergeable 0, unmergeable 2+: create single batches for unmergeable.
      # Mergeable 1, unmergeable 0: lone patch of unknown mergeability, retry.
      # Mergeable 1, unmergeable 1+: create single batches for both.
      # Mergeable 2+, unmergeable 0: bisect mergeable units.
      # Mergeable 2+, unmergeable 1+: one batch for mergeable, create for unmergeable.
      # Create batches for unmergeable units first so they fail first.
      # A bundle counts as unmergeable if any member is unmergeable.
      case isolate_unmergeable_units(units, repo_conn) do
        {[], [_]} ->
          :failed

        {[], multiple_unmergeable} ->
          Enum.each(multiple_unmergeable, fn unit ->
            clone_batch(unit, project.id, into)
          end)

          :retrying

        {[single_mergeable], multiple_unmergeable} ->
          Enum.each(multiple_unmergeable, fn unit ->
            clone_batch(unit, project.id, into)
          end)

          clone_batch(single_mergeable, project.id, into)
          :retrying

        {multiple_mergeable, []} ->
          bisect(multiple_mergeable, project.id, into)
          :retrying

        {multiple_mergeable, multiple_unmergeable} ->
          Enum.each(multiple_unmergeable, fn unit ->
            clone_batch(unit, project.id, into)
          end)

          clone_batch(Enum.concat(multiple_mergeable), project.id, into)
          :retrying
      end
    end
  end

  # A single unit that is a bundle (patches sharing a `bundle_id`). Such a
  # unit is indivisible, so a conflict can only be resolved by the user.
  defp single_bundle_unit?([unit]), do: link_patch(hd(unit)).bundle_id != nil
  defp single_bundle_unit?(_), do: false

  def clone_batch(patch_links, project_id, into_branch) do
    # Running batches have a sentinel priority of 100. Preserve the members'
    # requested priority, rather than copying that sentinel or resetting to zero.
    priority = patch_links |> Enum.map(&link_patch(&1).priority) |> Enum.max(fn -> 0 end)

    result =
      Repo.transaction(fn ->
        source_ids =
          patch_links
          |> Enum.map(&Map.get(&1, :batch_id))
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()

        # Serialize retry creation and recognize a child already committed
        # before a crash interrupted the parent's status/notification update.
        Repo.all(from(b in Batch, where: b.id in ^source_ids, order_by: b.id, lock: "FOR UPDATE"))

        existing =
          Repo.all(
            from(l in LinkPatchBatch,
              join: b in Batch,
              on: b.id == l.batch_id,
              where:
                b.project_id == ^project_id and b.id not in ^source_ids and
                  b.state in [:waiting, :running] and
                  l.patch_id in ^Enum.map(patch_links, & &1.patch_id),
              select: {l.patch_id, l.head_sha, b.id}
            )
          )

        represented = MapSet.new(existing, fn {id, head, _} -> {id, head} end)

        remaining =
          patch_links
          |> group_units()
          |> Enum.flat_map(fn unit ->
            {present, missing} =
              Enum.split_with(unit, &MapSet.member?(represented, {&1.patch_id, &1.head_sha}))

            if present != [] and missing != [], do: Repo.rollback(:partial_bundle_membership)
            missing
          end)

        if remaining == [] do
          case existing do
            [{_, _, id} | _] -> Repo.get!(Batch, id)
            [] -> nil
          end
        else
          batch = Repo.insert!(Batch.new(project_id, into_branch, priority))

          remaining
          |> Enum.map(
            &%{
              batch_id: batch.id,
              patch_id: &1.patch_id,
              reviewer: &1.reviewer,
              head_sha: &1.head_sha,
              manual_approval: &1.manual_approval
            }
          )
          |> Enum.map(&LinkPatchBatch.changeset(%LinkPatchBatch{}, &1))
          |> Enum.each(&Repo.insert!/1)

          batch
        end
      end)

    case result do
      {:ok, batch} ->
        batch

      {:error, reason} ->
        require Logger
        Logger.error("retry membership deferred project=#{project_id} reason=#{inspect(reason)}")
        nil
    end
  end

  @doc """
  Group patch links into atomic units, preserving the batch's link order.
  Links whose patches share a `bundle_id` form one unit. Every other link is
  a unit by itself.

  Links should have `patch` preloaded (LinkPatchBatch.from_batch/1 does).
  Otherwise each link falls back to its own patch query.
  """
  def group_units(patch_links) do
    patch_links
    |> Enum.reduce([], fn link, acc ->
      key = unit_key(link)

      case List.keyfind(acc, key, 0) do
        nil -> acc ++ [{key, [link]}]
        {_, links} -> List.keyreplace(acc, key, 0, {key, links ++ [link]})
      end
    end)
    |> Enum.map(fn {_key, links} -> links end)
  end

  defp unit_key(link) do
    case link_patch(link).bundle_id do
      nil -> {:solo, link.patch_id}
      bundle_id -> {:bundle, bundle_id}
    end
  end

  defp unit_is_single?(unit) do
    Enum.any?(unit, &link_patch(&1).is_single)
  end

  defp bisect(units, project_id, into) do
    count = Enum.count(units)

    {lo, hi} = Enum.split(units, div(count, 2))
    clone_batch(Enum.concat(lo), project_id, into)
    clone_batch(Enum.concat(hi), project_id, into)
  end

  defp isolate_unmergeable_units(units, repo_conn) do
    unit_map =
      Enum.group_by(units, fn unit ->
        Enum.all?(unit, &is_patch_mergeable(link_patch(&1), repo_conn))
      end)

    {unit_map[true] || [], unit_map[false] || []}
  end

  defp is_patch_mergeable(patch, repo_conn) do
    pr = GitHub.get_pr!(repo_conn, patch.pr_xref)
    (pr.mergeable == true || pr.mergeable == nil) && pr.draft != true
  end

  defp link_patch(%{patch: %Patch{} = patch}), do: patch
  defp link_patch(link), do: Repo.get!(Patch, link.patch_id)

  @spec get_repo_conn(%Project{}) :: {{:installation, number}, number}
  defp get_repo_conn(project) do
    Project.installation_connection(project.repo_xref, Repo)
  end
end
