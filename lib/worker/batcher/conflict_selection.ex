defmodule BorsNG.Worker.Batcher.ConflictSelection do
  @moduledoc "Skip conflicting atomic units before immutable candidate construction."
  import Ecto.Query
  alias BorsNG.Database.{Batch, LinkPatchBatch, Patch, Repo}
  alias BorsNG.Worker.{CandidateJournal, DeclarationNames, LocalGit}
  alias BorsNG.Worker.Batcher.{Bundles, Divider}

  def select(batch, links, base, conn) do
    units = Divider.group_units(links)
    identity = identity(batch, links, base)

    # Known batch-mate declaration collisions leave before the Git probe. A
    # unit without a known record is probed as before; the first unit can
    # never be skipped here, so the Git selection always has input.
    DeclarationNames.schedule(batch.project_id, links)
    skips = if DeclarationNames.enabled?(), do: declaration_skips(units, links), else: []
    rejected = Enum.flat_map(skips, & &1["members"])
    probed = Enum.reject(units, &(hd(members(&1)) in rejected))

    result =
      LocalGit.select(batch.project, conn, base, Enum.map(probed, &members/1), identity)
      |> merge_skips(skips)

    reconcile(batch, links, base, identity, result)
    |> tap(fn
      {:ok, _selected, %{"deferred_batch_id" => _}} ->
        Enum.each(skips, &DeclarationNames.notify(batch.project_id, conn, &1))

      _ ->
        :ok
    end)
  end

  @doc false
  def declaration_skips(units, links),
    do: DeclarationNames.collisions(units, DeclarationNames.records(links))

  # The pre-check's skips join the Git selection's own, ahead of them, as one
  # skipped list; the evidence keeps them apart for the journal.
  @doc false
  def merge_skips(result, []), do: result

  def merge_skips(%{"skipped" => skipped} = result, skips) when is_list(skipped) do
    result
    |> Map.put("skipped", skips ++ skipped)
    |> Map.put("declaration_collisions", skips)
  end

  def merge_skips(result, _), do: result

  @doc false
  def reconcile(batch, links, base, identity, result) do
    with ^identity <- identity(batch, links, base),
         %{"status" => "ok", "complete" => true, "identity" => ^identity} <- result,
         :ok <- validate(Divider.group_units(links), result) do
      apply_selection(batch, links, base, identity, result)
    else
      _ -> {:error, :invalid_selection}
    end
  end

  def identity(batch, links, base) do
    %{
      "batch_id" => batch.id,
      "base_sha" => base,
      "members_digest" => CandidateJournal.digest(snapshot(links))
    }
  end

  defp snapshot(links) do
    Enum.map(links, fn l ->
      %{
        link: Map.take(l, [:id, :batch_id, :patch_id, :head_sha, :reviewer, :manual_approval]),
        patch:
          Map.take(l.patch, [
            :commit,
            :manual_approval,
            :bundle_id,
            :stacked_on_id,
            :orchestration_approval_token,
            :open,
            :is_draft
          ])
      }
    end)
  end

  defp members(links),
    do: Enum.map(links, &%{"pr" => &1.patch.pr_xref, "head_sha" => &1.head_sha})

  # Validate a complete, order-preserving partition of whole bundle units.
  # This evidence only changes waiting membership; it cannot publish a tree.
  def validate(units, %{"accepted" => accepted, "skipped" => skipped})
      when is_list(accepted) and is_list(skipped) do
    input = Enum.map(units, &members/1)
    rejected = Enum.map(skipped, fn item -> if is_map(item), do: item["members"] end)

    valid =
      length(Enum.uniq(rejected)) == length(rejected) and
        Enum.all?(skipped, fn
          %{"members" => unit, "conflict" => %{"pr" => pr, "head_sha" => head, "kind" => kind}} ->
            unit in input and %{"pr" => pr, "head_sha" => head} in unit and
              kind in ["merge_conflict", "already_merged", "declaration_collision"]

          _ ->
            false
        end) and
        accepted == input |> Enum.reject(&(&1 in rejected)) |> List.flatten()

    if valid, do: :ok, else: {:error, :invalid_selection}
  end

  def validate(_, _), do: {:error, :invalid_selection}

  defp apply_selection(batch, links, base, identity, result) do
    # An empty selection has no candidate to dispatch. Retain the existing
    # singleton/bundle mergeability handling rather than renewing it forever.
    if result["skipped"] == [] or result["accepted"] == [] do
      {:ok, links, result}
    else
      Repo.transaction(fn ->
        current = Repo.one!(from(b in Batch, where: b.id == ^batch.id, lock: "FOR UPDATE"))
        ids = Enum.map(links, & &1.patch_id)
        Repo.all(from(p in Patch, where: p.id in ^ids, order_by: p.id, lock: "FOR UPDATE"))

        fresh = Repo.all(LinkPatchBatch.from_batch(batch.id)) |> Bundles.sort_links_for_merge()

        if current.state != :waiting or is_binary(current.commit) or
             identity(batch, fresh, base) != identity,
           do: Repo.rollback(:selection_membership_changed)

        rejected = result["skipped"] |> Enum.flat_map(& &1["members"]) |> MapSet.new()

        {deferred, selected} =
          Enum.split_with(fresh, &MapSet.member?(rejected, hd(members([&1]))))

        # One deferred group, with bundles intact. A merge conflict is not a
        # failed review/CI verdict and must not turn N conflicts into N builds.
        retry =
          Divider.clone_batch(
            deferred,
            batch.project_id,
            batch.into_branch,
            "merge_conflict_deferred"
          )

        if is_nil(retry), do: Repo.rollback(:selection_retry_unavailable)

        retry_members = Repo.all(LinkPatchBatch.from_batch(retry.id)) |> members() |> MapSet.new()
        if retry_members != rejected, do: Repo.rollback(:selection_retry_membership)

        deferred_ids = Enum.map(deferred, & &1.id)
        Repo.delete_all(from(l in LinkPatchBatch, where: l.id in ^deferred_ids))
        {selected, Map.put(result, "deferred_batch_id", retry.id)}
      end)
      |> case do
        {:ok, {selected, evidence}} -> {:ok, selected, evidence}
        {:error, reason} -> {:error, reason}
      end
    end
  end
end
