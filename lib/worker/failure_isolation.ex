defmodule BorsNG.Worker.FailureIsolation do
  @moduledoc "Conservative scheduling hints; only an indivisible failed retry is quarantined."
  import Ecto.Query
  alias BorsNG.Database.{Batch, CandidateEvent, Repo}
  alias BorsNG.Worker.{CandidateJournal, Batcher.Divider}

  def enabled?, do: System.get_env("BORS_FAILURE_DIAGNOSTICS") != "off"

  # Legacy journals used one completeness flag. New helpers can retain exact
  # ownership when optional imports are unknown; only retry planning uses it.
  def ownership_complete?(%{"ownership_complete" => value}), do: value == true
  def ownership_complete?(%{"complete" => true}), do: true
  def ownership_complete?(_), do: false

  defp validated_hints(nil, _, %{"ownership_complete" => true}), do: nil

  defp validated_hints(candidate, result, %{"ownership_complete" => true} = hints) do
    identity =
      BorsNG.Worker.CandidateAssembler.identity(candidate) |> Jason.encode!() |> Jason.decode!()

    members = Enum.map(candidate.snapshot["members"], &Map.take(&1, ["pr", "head_sha"]))

    provenance =
      identity
      |> Map.merge(%{
        "commit" => result["commit"] || result[:commit],
        "tree" => result["tree"] || result[:tree],
        "members" => members
      })

    binding = CandidateJournal.commit_binding(candidate)

    if hints["provenance"] == provenance and
         (binding == %{} or
            Map.take(binding, ["commit", "tree"]) == Map.take(provenance, ["commit", "tree"])) and
         Enum.map(hints["changed_files"] || [], &Map.take(&1, ["pr", "head_sha"])) == members,
       do: hints
  end

  defp validated_hints(_, _, hints), do: hints

  # Only a strict multi-unit retry is eligible for this shortcut. A singleton
  # still goes to authoritative CI; source inventory alone never quarantines.
  def preflight(links, batch, result) do
    candidate = CandidateJournal.latest(batch.id)
    hints = validated_hints(candidate, result, get_in(result, [:evidence, "diagnostic_hints"]))

    if enabled?() and is_map(hints) and hints["complete"] == true and
         (hints["missing_imports"] || []) != [] and
         match?({:ok, _, "missing_import"}, partition(links, hints, nil)) do
      # There is no running CI binding to replay here. Retire the waiting
      # parent in the same transaction as every child: after a crash it must
      # never reconstruct beside live children against a newly advanced base.
      {:ok, :partitioned} =
        Repo.transaction(fn ->
          parent = Repo.one!(from(b in Batch, where: b.id == ^batch.id, lock: "FOR UPDATE"))
          candidate = CandidateJournal.latest(batch.id)

          CandidateJournal.event(candidate, "local_import_failure", "bors.local_git", %{
            assembly_identity: BorsNG.Worker.CandidateAssembler.identity(candidate),
            commit: result.commit,
            tree: result.tree,
            missing_imports: hints["missing_imports"],
            authority: "retry_only",
            proof_authority: false,
            ci_dispatched: false,
            parent_retired_atomically: true
          })

          :retrying = split(links, batch)
          parent |> Batch.changeset(%{state: :error, commit: nil}) |> Repo.update!()
          :partitioned
        end)

      :partitioned
    else
      :dispatch
    end
  end

  def hints(candidate) do
    if candidate do
      Repo.all(
        from(e in CandidateEvent,
          where: e.candidate_id == ^candidate.id and e.source == "bors.local_git",
          order_by: [desc: e.id]
        )
      )
      |> Enum.find(&(&1.kind in ["local_assembled", "local_conflict"]))
      |> case do
        nil ->
          nil

        e ->
          case e.kind do
            "local_assembled" ->
              validated_hints(
                candidate,
                e.details,
                get_in(e.details, ["evidence", "diagnostic_hints"])
              )

            "local_conflict" ->
              e.details["hints"]
          end
      end
    end
  end

  def split(links, batch, conflict \\ nil) do
    candidate = CandidateJournal.latest(batch.id)
    links = ordered_links(links, candidate)

    hints = if enabled?() and not uncertain_terminal?(candidate), do: hints(candidate)

    hints =
      if is_map(hints) do
        case BorsNG.Worker.FailureDiagnostics.suspects(candidate, hints) do
          {:ok, prs} -> Map.put(hints, "compiler_suspects", prs)
          _ -> hints
        end
      end

    case partition(links, hints, conflict) do
      {:ok, groups, reason} ->
        CandidateJournal.event(candidate, "failure_partition", "bors.failure_isolation", %{
          strategy: reason,
          groups: Enum.map(groups, fn group -> Enum.map(group, & &1.patch.pr_xref) end),
          ownership_complete: ownership_complete?(hints),
          dependencies_complete: Map.get(hints, "dependencies_complete", hints["complete"]),
          authority: "retry_only"
        })

        Enum.each(groups, fn group ->
          if is_nil(Divider.clone_batch(group, batch.project_id, batch.into_branch, reason)),
            do: throw(:retry_membership_unavailable)
        end)

        :retrying

      :fallback ->
        CandidateJournal.event(candidate, "failure_partition", "bors.failure_isolation", %{
          strategy: "deterministic_bisection",
          reason: "unknown, ambiguous or non-narrowing diagnostics"
        })

        if conflict &&
             (not enabled?() or conflict["kind"] != "merge_conflict" or
                length(Divider.group_units(links)) == 1),
           do: Divider.split_batch_with_conflicts(links, batch),
           else: Divider.split_batch(links, batch)
    end
  end

  # SQL row order is unspecified. Freeze split order to the actual recorded
  # assembly inputs; historical batches without a candidate use durable link IDs.
  defp ordered_links(links, candidate) do
    members = if candidate, do: candidate.snapshot["members"], else: []

    order =
      Map.new(Enum.with_index(members), fn {member, index} -> {member["link_id"], index} end)

    Enum.sort_by(links, &{Map.get(order, &1.id, map_size(order)), &1.id, &1.patch_id})
  end

  defp uncertain_terminal?(nil), do: false

  defp uncertain_terminal?(candidate) do
    event =
      Repo.one(
        from(e in CandidateEvent,
          where:
            e.candidate_id == ^candidate.id and e.kind == "failure_classified" and
              e.source == "bors.trusted_failure",
          order_by: [desc: e.id],
          limit: 1
        )
      )

    event != nil and get_in(event.details, ["evidence", "diagnostic"]) != nil
  end

  # Pure planner. Every result is a strict partition of atomic bundle units.
  # No diagnostic can discard a member or prove that an individual is failing.
  def partition(links, hints, conflict) when is_map(hints) do
    units = Divider.group_units(links)
    changed = hints["changed_files"] || []
    heads = Map.new(links, &{&1.patch.pr_xref, &1.head_sha})
    evidence_heads = Map.new(changed, &{&1["pr"], &1["head_sha"]})

    if ownership_complete?(hints) and length(changed) == length(links) and
         length(units) > 1 and heads == evidence_heads and
         Enum.all?(links, &(&1.head_sha == &1.patch.commit)) do
      numbered = Enum.with_index(units)

      owner =
        Map.new(numbered |> Enum.flat_map(fn {u, i} -> Enum.map(u, &{&1.patch.pr_xref, i}) end))

      dependencies_complete = Map.get(hints, "dependencies_complete", hints["complete"]) == true
      edges = if dependencies_complete, do: hints["import_edges"] || [], else: []

      # Shared changed paths and imports identify possible interacting PRs.
      path_owners =
        changed
        |> Enum.flat_map(fn c -> Enum.map(c["paths"], &{&1, owner[c["pr"]]}) end)
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

      pairs = Enum.flat_map(path_owners, fn {_, ids} -> for a <- ids, b <- ids, do: {a, b} end)
      pairs = pairs ++ Enum.map(edges, fn [a, b] -> {owner[a], owner[b]} end)
      ids = Enum.map(numbered, &elem(&1, 1))
      components = components(ids, pairs)

      suspects =
        if conflict do
          [owner[conflict["pr"]]]
        else
          Enum.map(
            if(dependencies_complete, do: hints["missing_imports"] || [], else: []),
            &owner[&1["pr"]]
          ) ++
            Enum.map(hints["compiler_suspects"] || [], &owner[&1])
        end
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      # Preserve provider/importer relationships for missing-import retries.
      suspects =
        if conflict,
          do: suspects,
          else:
            components |> Enum.filter(&Enum.any?(&1, fn i -> i in suspects end)) |> List.flatten()

      {parts, reason} =
        cond do
          suspects != [] and length(suspects) < length(units) ->
            {[suspects, ids -- suspects],
             cond do
               conflict -> "structured_conflict"
               hints["compiler_suspects"] != nil -> "compiler_diagnostics"
               true -> "missing_import"
             end}

          hints["compiler_suspects"] != nil ->
            {[], nil}

          length(components) > 1 and Enum.any?(components, &(length(&1) > 1)) ->
            [first | rest] = Enum.sort_by(components, &{-length(&1), hd(&1)})
            {[first, List.flatten(rest)], "dependency_component"}

          true ->
            {[], nil}
        end

      # is_single is an additional scheduling constraint on a whole bundle.
      groups =
        parts
        |> Enum.flat_map(fn part ->
          {single, other} =
            Enum.split_with(part, fn i -> Enum.any?(Enum.at(units, i), & &1.patch.is_single) end)

          Enum.map(single, &[&1]) ++ if(other == [], do: [], else: [other])
        end)
        |> Enum.map(fn part ->
          ids |> Enum.filter(&(&1 in part)) |> Enum.flat_map(&Enum.at(units, &1))
        end)

      if length(groups) > 1, do: {:ok, groups, reason}, else: :fallback
    else
      :fallback
    end
  end

  def partition(_, _, _), do: :fallback

  defp components([], _), do: []

  defp components([id | rest], edges) do
    connected = expand([id], edges)
    [Enum.sort(connected) | components(rest -- connected, edges)]
  end

  defp expand(ids, edges) do
    next =
      (ids ++ Enum.flat_map(edges, fn {a, b} -> if a in ids or b in ids, do: [a, b], else: [] end))
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    if length(next) == length(ids), do: next, else: expand(next, edges)
  end

  def classified(batch, classification, details \\ %{}) do
    CandidateJournal.event(
      CandidateJournal.current(batch),
      "failure_classified",
      "bors.trusted_failure",
      %{
        classification: classification,
        evidence: details,
        commit: batch.commit,
        base_sha: batch.base_sha,
        proof_authority: false
      }
    )
  end

  # Reconsideration reuses only an existing App-owned, review-safe exact-head
  # policy decision, then passes ordinary live admission. Human scope approvals
  # and terminal bundles still require fresh review. Uncertain comparisons hold.
  def reconsider(conn, patch, decision) do
    metadata =
      if get_in(patch.merge_eligibility || %{}, ["head_sha"]) == patch.commit,
        do: patch.merge_eligibility,
        else: %{}

    count = metadata["failure_retry_count"] || 0

    with true <- enabled?(),
         true <- is_integer(count) and count >= 0 and count < 3,
         nil <- patch.manual_approval,
         nil <- patch.bundle_id,
         true <- decision["eligible"] and decision["review_safe"],
         %{
           "state" => "failed",
           "head_sha" => head,
           "quarantine" => q,
           "batch_id" => batch_id,
           "reason" => reason
         } <- patch.queue_approval,
         true <- reason in ["build", "conflict"],
         %BorsNG.Database.Batch{state: state, project_id: project_id, into_branch: branch} <-
           Repo.get(BorsNG.Database.Batch, batch_id),
         true <-
           state in [:error, :conflict] and project_id == patch.project_id and
             branch == patch.into_branch,
         false <- active?(patch),
         true <- head == patch.commit and not patch.queue_pending,
         true <- q["diagnostics_complete"] == true and is_binary(q["reviewer"]),
         base when is_binary(base) <- q["base_sha"],
         {:ok, %{commit: current}} <- BorsNG.GitHub.get_recovery_base(conn),
         true <-
           current != base and current != (patch.merge_eligibility || %{})["failure_retry_base"],
         {:ok, changed} <- BorsNG.GitHub.failure_base_changes(conn, base, current),
         relevant when relevant != [] <- Enum.filter(changed, &(&1 in q["paths"])) do
      candidate = Repo.get(BorsNG.Database.Candidate, q["candidate_id"])
      # A blocked admission can revisit the same comparison. Keep evidence
      # append-only but deduplicate its exact head/base observation.
      already_recorded =
        if candidate,
          do:
            Repo.exists?(
              from(e in CandidateEvent,
                where:
                  e.candidate_id == ^candidate.id and e.kind == "quarantine_reconsidered" and
                    e.details["current_base"] == ^current
              )
            ),
          else: true

      unless already_recorded do
        CandidateJournal.event(
          candidate,
          "quarantine_reconsidered",
          "github.compare",
          %{
            head_sha: head,
            failed_base: base,
            current_base: current,
            changed_paths: relevant,
            authority: "ordinary_admission_required"
          }
        )
      end

      {:retry,
       %{
         "failure_retry_base" => current,
         "failure_retry_candidate" => q["candidate_id"],
         "failure_retry_count" => count + 1
       }}
    else
      _ -> :defer
    end
  end

  defp active?(patch) do
    Repo.exists?(
      from(l in BorsNG.Database.LinkPatchBatch,
        join: b in BorsNG.Database.Batch,
        on: b.id == l.batch_id,
        where:
          l.patch_id == ^patch.id and l.head_sha == ^patch.commit and
            b.state in [:waiting, :running]
      )
    )
  end

  def quarantine(patch, batch, reason) do
    candidate = CandidateJournal.latest(batch.id)
    hints = hints(candidate) || %{}
    members = if candidate, do: candidate.snapshot["members"], else: []
    member = Enum.find(members, &(&1["patch_id"] == patch.id)) || %{}
    paths = Enum.flat_map(hints["changed_files"] || [], & &1["paths"])

    %{
      "candidate_id" => if(candidate, do: candidate.id),
      "head_sha" => member["head_sha"],
      "base_sha" => if(candidate, do: candidate.snapshot["base_sha"], else: batch.base_sha),
      "reason" =>
        "indivisible #{reason} failure at this exact head/base; bundle members are not individually blamed",
      "reviewer" => member["reviewer"],
      "paths" =>
        Enum.uniq(
          paths ++
            (hints["dependency_paths"] || []) ++
            ~w(lean-toolchain lakefile.lean lakefile.toml lake-manifest.json)
        ),
      "diagnostics_complete" =>
        hints["complete"] == true and member["head_sha"] == patch.commit and
          member["assembly_head_sha"] == patch.commit
    }
  end
end
