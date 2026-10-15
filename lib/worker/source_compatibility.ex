defmodule BorsNG.Worker.SourceCompatibility do
  @moduledoc "Bounded immutable source checks feeding the existing head-bound rebase handoff."
  import Ecto.Query
  alias BorsNG.Database.{Batch, CandidateEvent, LinkPatchBatch, Patch, Project, Repo}
  alias BorsNG.GitHub
  alias BorsNG.Worker.{Batcher, LocalGit}
  alias BorsNG.Worker.Orchestration.Outbox
  @policy "local-source-v2"
  @keep ~w(keep hold wip human do-not-close)

  def enabled?, do: System.get_env("BORS_SOURCE_COMPATIBILITY", "on") != "off"

  def wake(project, main \\ nil)

  def wake(%Project{name: "TauCetiProject/TauCeti"} = project, main) do
    if Outbox.enabled?() and enabled?() do
      # The executor reads actual main. Promotion notifications only make this
      # one project-level job due; repeated notifications never fan out per PR.
      if main do
        current = Repo.get!(Project, project.id)
        state = current.source_compatibility || %{}

        if state["base_sha"] != main,
          do:
            current
            |> Ecto.Changeset.change(source_compatibility: Map.put(state, "required_sha", main))
            |> Repo.update!()
      end

      at = if main, do: Outbox.now(), else: Outbox.now() + 60
      Outbox.enqueue(project.id, "compatibility", nil, "compatibility:#{project.id}", at)
    end
  end

  def wake(_, _), do: :ok

  def chunk(project_id, checker \\ &LocalGit.compatibility/4) do
    project = Repo.get!(Project, project_id)
    conn = Project.installation_connection(project.repo_xref, Repo)
    previous = project.source_compatibility || %{}
    # Ordinary heartbeat reconciliation handles missed events and restarts.
    if previous["checked_at"] == nil or Outbox.now() - previous["checked_at"] >= 300 or
         (previous["required_sha"] != nil and previous["required_sha"] != previous["base_sha"]) do
      with true <- enabled?(),
           "local" <- LocalGit.mode(project, "main", conn),
           {:ok, %{commit: base}} <- GitHub.get_branch(conn, "main") do
        cursor = previous["cursor"] || 0

        active =
          from(l in LinkPatchBatch,
            join: b in Batch,
            on: l.batch_id == b.id,
            where: b.project_id == ^project_id and b.state in [:waiting, :running],
            select: l.patch_id
          )

        eligible =
          from(p in Patch,
            where:
              p.project_id == ^project_id and p.open and
                not p.is_draft and p.into_branch == "main" and is_nil(p.bundle_id) and
                (p.id in subquery(active) or p.queue_pending or
                   p.merge_eligibility["eligible"] == true),
            order_by: p.id
          )

        stale =
          from(p in eligible,
            where:
              is_nil(p.source_compatibility) or
                p.source_compatibility["projection_pending"] == true or
                type(p.source_compatibility["base_sha"], :string) != ^base or
                type(p.source_compatibility["head_sha"], :string) != p.commit or
                type(p.source_compatibility["policy"], :string) != ^@policy or
                (type(p.source_compatibility["status"], :string) in ["conflict", "missing_source"] and
                   is_nil(p.source_compatibility["repair_head"])) or
                (type(p.source_compatibility["status"], :string) == "pass" and
                   not is_nil(p.source_compatibility["repair_head"]) and
                   type(p.source_compatibility["repair_base"], :string) != ^base)
          )

        newer = Repo.all(from(p in stale, where: p.id > ^cursor, limit: 4))

        selected =
          newer ++
            Repo.all(
              from(p in stale,
                where: p.id <= ^cursor,
                limit: ^(4 - length(newer))
              )
            )

        check(project, conn, base, selected, checker)

        Enum.each(selected, fn patch ->
          current = Repo.get!(Patch, patch.id)

          if blocked?(current, patch.commit),
            do: Batcher.do_handle_cast({:cancel, patch.id, :source_compatibility}, project.id)
        end)

        # Never restart at PR 1 when main moves. Stale results stay stale, and
        # every chunk rotates fairly; admission does not wait for this sweep.
        progress = %{
          "base_sha" => base,
          "policy" => @policy,
          "cursor" => if(selected == [], do: cursor, else: List.last(selected).id),
          "checked_at" => if(length(selected) == 4, do: Outbox.now() - 240, else: Outbox.now())
        }

        project |> Ecto.Changeset.change(source_compatibility: progress) |> Repo.update!()
      else
        _ -> :ok
      end
    end
  end

  def admit(%Project{name: "TauCetiProject/TauCeti"} = project, conn, batch, links, base) do
    if enabled?() and batch.into_branch == "main" and
         LocalGit.mode(project, "main", conn) == "local" do
      with {:ok, %{commit: ^base}} <- GitHub.get_branch(conn, "main") do
        # Bounded selected heads get a just-in-time exact check. Bundles and
        # excess/unknown coverage retain authoritative candidate validation.
        patches =
          links |> Enum.map(& &1.patch) |> Enum.filter(&is_nil(&1.bundle_id)) |> Enum.take(32)

        check(project, conn, base, patches)

        Enum.reject(links, fn link ->
          current = Repo.get!(Patch, link.patch_id)

          if blocked?(current, link.head_sha) do
            Repo.delete!(link)
            BorsNG.Worker.ManualApproval.revoke(current.id)
            BorsNG.Worker.QueueApproval.stop([current], :source_compatibility)
            BorsNG.Worker.Labeler.reconcile_queue(conn, batch.into_branch, [current])
            true
          else
            false
          end
        end)
      else
        _ -> links
      end
    else
      links
    end
  end

  def admit(_, _, _, links, _), do: links

  def requested?(patch, head),
    do: get_in(patch.source_compatibility || %{}, ["repair_head"]) == head

  defp blocked?(patch, head),
    do:
      requested?(patch, head) or get_in(patch.source_compatibility || %{}, ["hold_head"]) == head

  def check(project, conn, base, patches, checker \\ &LocalGit.compatibility/4)
  def check(_, _, _, [], _), do: :ok

  def check(project, conn, base, patches, checker) do
    Enum.each(patches, fn patch ->
      if requested?(patch, patch.commit) and
           patch.source_compatibility["projection_pending"] == true,
         do: project_request(conn, patch)
    end)

    pending =
      Enum.reject(patches, fn patch ->
        r = patch.source_compatibility || %{}

        resolving =
          r["status"] == "pass" and requested?(patch, patch.commit) and
            r["repair_policy"] == @policy and r["repair_base"] != base

        BorsNG.Worker.BumpRecovery.automatic?(patch) or
          (r["base_sha"] == base and r["head_sha"] == patch.commit and r["policy"] == @policy and
             not resolving and
             (r["status"] not in ["conflict", "missing_source"] or requested?(patch, patch.commit)))
      end)

    members = Enum.map(pending, &%{pr: &1.pr_xref, head_sha: &1.commit})

    if members != [] do
      case checker.(project, conn, base, members) do
        %{"status" => "ok", "results" => results} ->
          apply_results(project, conn, base, pending, results)

        failure ->
          # A bounded fetch/lock/auth failure is unknown coverage, not a pass
          # and not an invisible completed sweep. Keep it exact-head/base bound
          # and leave authoritative staging available without repeated scans.
          reason =
            case failure do
              %{"details" => %{"kind" => kind}}
              when kind in ~w(timeout lock_timeout git_failed mirror_limit output_limit helper_failed helper_or_auth_failed helper_unavailable) ->
                "source_helper_" <> kind

              _ ->
                "source_helper_unavailable"
            end

          results =
            Enum.map(members, fn member ->
              %{
                "pr" => member.pr,
                "head_sha" => member.head_sha,
                "base_sha" => base,
                "policy" => @policy,
                "status" => "unknown",
                "reason" => reason
              }
            end)

          apply_results(project, conn, base, pending, results)
      end
    end
  end

  @doc false
  def apply_results(project, conn, base, patches, results) do
    with {:ok, %{commit: ^base}} <- GitHub.get_branch(conn, "main") do
      Enum.each(patches, fn patch ->
        result =
          Enum.find(
            results,
            &(&1["pr"] == patch.pr_xref and &1["head_sha"] == patch.commit and
                &1["base_sha"] == base and &1["policy"] == @policy)
          )

        if result do
          with false <- Outbox.withdrawal_pending?(project.id, [patch.id]),
               {:ok, pr} <- GitHub.get_pr(conn, patch.pr_xref),
               true <- pr.head_sha == patch.commit and pr.state == :open and not pr.draft,
               current when not is_nil(current) <- Repo.get(Patch, patch.id),
               true <- current.commit == patch.commit do
            result =
              Map.merge(
                result,
                repair_metadata(current)
              )

            result = maybe_resolve(project, conn, current, result)

            current =
              current |> Ecto.Changeset.change(source_compatibility: result) |> Repo.update!()

            if result["status"] in ["conflict", "missing_source"],
              do: handoff(project, conn, current, result)
          else
            _ -> :ok
          end
        end
      end)
    else
      _ -> :ok
    end
  end

  defp maybe_resolve(project, conn, patch, result) do
    if result["status"] == "pass" and result["repair_head"] == patch.commit and
         result["repair_policy"] == @policy and result["repair_base"] != result["base_sha"] do
      # Cancel a delayed source notice before editing already-published markers.
      # This executor owns the repository lane, so no sibling notice is running.
      key =
        "source-repair:#{project.id}:#{patch.pr_xref}:#{patch.commit}:#{result["repair_base"]}"

      Repo.update_all(
        from(j in BorsNG.Database.OrchestrationJob,
          where: j.project_id == ^project.id and j.dedup_key == ^key and j.state == "pending"
        ),
        set: [state: "done"]
      )

      with :ok <-
             GitHub.resolve_source_request(conn, patch.pr_xref, patch.commit, result["base_sha"]),
           :ok <- GitHub.remove_label(conn, patch.pr_xref, "needs-rebase") do
        # Re-offer automatic admission through ordinary authorization. Never
        # recreate a revoked human approval or undo a subsequent explicit r-.
        if get_in(patch.queue_approval || %{}, ["reason"]) == "source_compatibility",
          do: patch |> Patch.changeset(%{merge_eligibility_id: nil}) |> Repo.update!()

        Map.drop(result, ["repair_head", "repair_base", "repair_policy", "projection_pending"])
      else
        _ -> result
      end
    else
      result
    end
  end

  def journal(candidate, links) do
    observations =
      Enum.map(links, fn link ->
        patch = Repo.get!(Patch, link.patch_id)
        result = patch.source_compatibility || %{}

        if result["head_sha"] == link.head_sha and
             result["base_sha"] == candidate.snapshot["base_sha"] and result["policy"] == @policy do
          Map.take(result, ~w(pr head_sha base_sha policy status reason coverage))
        else
          %{
            "pr" => patch.pr_xref,
            "head_sha" => link.head_sha,
            "base_sha" => candidate.snapshot["base_sha"],
            "policy" => @policy,
            "status" => "unknown",
            "reason" => "no exact singleton coverage for this candidate base"
          }
        end
      end)

    BorsNG.Worker.CandidateJournal.event(
      candidate,
      "source_compatibility_checked",
      "bors.source",
      %{results: observations, proof_authority: false}
    )
  end

  defp handoff(project, conn, patch, result) do
    with {:ok, labels} <- GitHub.get_labels(conn, patch.pr_xref),
         true <- MapSet.disjoint?(MapSet.new(labels, &String.downcase/1), MapSet.new(@keep)),
         true <- is_nil(patch.bundle_id) do
      unless requested?(patch, patch.commit) do
        body = request(patch.commit, result)
        # Reuse durable notices, including crash/API-outage retry and dedup.
        marker = "<!--tauceti-rebase:v1 #{patch.commit}-->"

        Outbox.enqueue(
          project.id,
          "notice",
          {conn, patch.pr_xref, body, marker},
          "source-repair:#{project.id}:#{patch.pr_xref}:#{patch.commit}:#{result["base_sha"]}"
        )

        patch
        |> Ecto.Changeset.change(
          source_compatibility:
            Map.merge(result, %{
              "repair_head" => patch.commit,
              "repair_base" => result["base_sha"],
              "repair_policy" => result["policy"],
              "projection_pending" => true
            })
        )
        |> Repo.update!()
      end

      # A label is a projection, never a passing compatibility status. Its
      # current head is withdrawn through the existing serialized cancellation.
      project_request(conn, Repo.get!(Patch, patch.id))
    else
      false ->
        held =
          Map.merge(result, %{
            "hold_head" => patch.commit,
            "action" => "held; author/maintainer attention required"
          })

        patch |> Ecto.Changeset.change(source_compatibility: held) |> Repo.update!()

      _ ->
        :ok
    end
  end

  defp repair_metadata(patch) do
    if requested?(patch, patch.commit),
      do:
        Map.take(
          patch.source_compatibility || %{},
          ~w(repair_head repair_base repair_policy projection_pending)
        ),
      else: %{}
  end

  defp project_request(conn, patch) do
    with false <- Outbox.withdrawal_pending?(patch.project_id, [patch.id]),
         {:ok, pr} <- GitHub.get_pr(conn, patch.pr_xref),
         true <- pr.head_sha == patch.commit and pr.state == :open and not pr.draft do
      pending = GitHub.add_labels(conn, patch.pr_xref, ["needs-rebase"]) != :ok
      current = Repo.get!(Patch, patch.id)

      if current.commit == patch.commit do
        current
        |> Ecto.Changeset.change(
          source_compatibility:
            Map.put(current.source_compatibility || %{}, "projection_pending", pending)
        )
        |> Repo.update!()
      end
    else
      _ -> :ok
    end
  end

  # Compiler/type/API/duplicate failures remain authoritative-CI work. Only an
  # exact single-member terminal candidate can use this handoff, never a suspect
  # partition or a quarantine label on its own.
  def proven_failure(batch, links, conn) do
    project = Repo.get!(Project, batch.project_id)
    candidate = BorsNG.Worker.CandidateJournal.current(batch)

    with true <- enabled?(),
         "TauCetiProject/TauCeti" <- project.name,
         "main" <- batch.into_branch,
         [link] <- links,
         false <- Outbox.withdrawal_pending?(project.id, [link.patch_id]),
         nil <- link.patch.bundle_id,
         false <- BorsNG.Worker.BumpRecovery.automatic?(link.patch),
         true <- not is_nil(candidate),
         [member] <- candidate.snapshot["members"],
         true <- member["head_sha"] == link.head_sha and link.head_sha == link.patch.commit,
         %CandidateEvent{details: details} <-
           Repo.one(
             from(e in CandidateEvent,
               where:
                 e.candidate_id == ^candidate.id and e.kind == "failure_classified" and
                   e.source == "bors.trusted_failure",
               order_by: [desc: e.id],
               limit: 1
             )
           ),
         "candidate_failure" <- details["classification"],
         true <- details["commit"] == batch.commit and details["base_sha"] == batch.base_sha,
         %{"conclusion" => "failure", "run_id" => run, "diagnostic" => nil} = evidence <-
           Map.put_new(details["evidence"] || %{}, "diagnostic", nil),
         true <- is_integer(run) and run > 0,
         true <-
           Enum.any?(
             evidence["failed_steps"] || [],
             &(&1["name"] in [
                 "Preflight exact-candidate import headers",
                 "Build exact candidate under bwrap with workflow-pinned audits, offline"
               ])
           ),
         {:ok, pr} <- GitHub.get_pr(conn, link.patch.pr_xref),
         true <- pr.head_sha == link.head_sha and pr.state == :open and not pr.draft do
      result = %{
        "head_sha" => link.head_sha,
        "base_sha" => batch.base_sha,
        "policy" => "authoritative-individual-v1",
        "status" => "integrated_failure",
        "run_url" => "https://github.com/TauCetiProject/TauCeti/actions/runs/#{run}"
      }

      patch = Repo.get!(Patch, link.patch_id)

      if patch.commit == link.head_sha do
        result =
          Map.merge(
            result,
            repair_metadata(patch)
          )

        patch = patch |> Ecto.Changeset.change(source_compatibility: result) |> Repo.update!()
        handoff(project, conn, patch, result)
      end
    else
      _ -> :ok
    end
  end

  def request(head, result) do
    details =
      case result["status"] do
        "missing_source" ->
          Enum.map_join(Enum.take(result["missing_imports"], 8), "\n", fn finding ->
            "- `#{finding["path"]}` imports missing `#{finding["module"]}` (`#{finding["provider_path"]}`)."
          end)

        "conflict" ->
          "Textual merge conflicts: " <> Enum.join(Enum.take(result["paths"], 8), ", ")

        "integrated_failure" ->
          "Authoritative exact single-PR integrated build failed: #{result["run_url"]}. " <>
            "Read its actual diagnostics to repair API/proof/duplicate-declaration failures; no filename proximity inference is used."
      end

    "Merge-queue recovery for head `#{String.slice(head, 0, 7)}`: exact individual source compatibility failed.\n\n" <>
      "Tested main: `#{result["base_sha"]}`. Checker policy: `#{result["policy"]}`.\n#{details}\n\n" <>
      "The prospective tree includes this PR's added modules. This does not change its existing head-CI build status. " <>
      "Its author or tending worker should integrate one current main snapshot, repair imports/APIs/proofs as necessary, " <>
      "and pass build and axiom checks before a compare-and-swap safe push. Git rebase alone is not proof of repair. " <>
      "Normal fresh-head CI and review then apply. Existing ownership/fork/human-path restrictions, claims and per-PR retry caps apply; " <>
      "untended or unwritable heads require their author/maintainer. Add keep to pause.\n\n<!--tauceti-rebase:v1 #{head}-->"
  end
end
