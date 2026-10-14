defmodule BorsNG.Worker.FailureIsolation do
  @moduledoc "Conservative scheduling hints; only an indivisible failed retry is quarantined."
  import Ecto.Query
  alias BorsNG.Database.{CandidateEvent, Repo}
  alias BorsNG.Worker.{CandidateJournal, Batcher.Divider}

  def enabled?, do: System.get_env("BORS_FAILURE_DIAGNOSTICS") != "off"

  def hints(candidate) do
    if candidate do
      Repo.all(
        from(e in CandidateEvent,
          where: e.candidate_id == ^candidate.id and e.source == "bors.local_git",
          order_by: [desc: e.id]
        )
      )
      |> Enum.find_value(fn e ->
        case e.kind do
          "local_assembled" -> get_in(e.details, ["evidence", "diagnostic_hints"])
          "local_conflict" -> e.details["hints"]
          _ -> nil
        end
      end)
    end
  end

  def split(links, batch, conflict \\ nil) do
    candidate = CandidateJournal.latest(batch.id)

    case partition(links, if(enabled?(), do: hints(candidate)), conflict) do
      {:ok, groups, reason} ->
        CandidateJournal.event(candidate, "failure_partition", "bors.failure_isolation", %{
          strategy: reason,
          groups: Enum.map(groups, fn group -> Enum.map(group, & &1.patch.pr_xref) end),
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

        if conflict && length(Divider.group_units(links)) == 1,
          do: Divider.split_batch_with_conflicts(links, batch),
          else: Divider.split_batch(links, batch)
    end
  end

  # Pure planner. Every result is a strict partition of atomic bundle units.
  # No diagnostic can discard a member or prove that an individual is failing.
  def partition(links, %{"complete" => true} = hints, conflict) do
    units = Divider.group_units(links)
    changed = hints["changed_files"] || []
    heads = Map.new(links, &{&1.patch.pr_xref, &1.head_sha})
    evidence_heads = Map.new(changed, &{&1["pr"], &1["head_sha"]})

    if length(units) > 1 and heads == evidence_heads and
         Enum.all?(links, &(&1.head_sha == &1.patch.commit)) do
      numbered = Enum.with_index(units)

      owner =
        Map.new(numbered |> Enum.flat_map(fn {u, i} -> Enum.map(u, &{&1.patch.pr_xref, i}) end))

      edges = hints["import_edges"] || []

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
          Enum.map(hints["missing_imports"] || [], &owner[&1["pr"]])
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
             if(conflict, do: "structured_conflict", else: "missing_import")}

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
    with true <- enabled?(),
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
      CandidateJournal.event(
        Repo.get(BorsNG.Database.Candidate, q["candidate_id"]),
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

      {:retry, %{"failure_retry_base" => current, "failure_retry_candidate" => q["candidate_id"]}}
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
      "head_sha" => patch.commit,
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
      "diagnostics_complete" => hints["complete"] == true
    }
  end
end
