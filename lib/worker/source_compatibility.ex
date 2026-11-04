defmodule BorsNG.Worker.SourceCompatibility do
  @moduledoc "Bounded immutable source checks feeding the existing head-bound rebase handoff."
  import Ecto.Query
  alias BorsNG.Database.{Batch, Candidate, CandidateEvent, LinkPatchBatch, Patch, Project, Repo}
  alias BorsNG.GitHub
  alias BorsNG.Worker.{Batcher, LocalGit}
  alias BorsNG.Worker.Orchestration.Outbox
  @policy "local-source-v3"
  @provisional_policy "provisional-diagnostic-v1"
  @keep ~w(keep hold wip human do-not-close)
  @background_heads 4

  def enabled?, do: System.get_env("BORS_SOURCE_COMPATIBILITY", "on") != "off"

  def wake(project, main \\ nil)

  def wake(%Project{} = project, main) do
    if project.name == BorsNG.ProjectConfig.repo() and Outbox.enabled?() and enabled?() do
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

  def chunk(project_id, checker \\ &LocalGit.compatibility(&1, &2, &3, &4, 8, 30)) do
    project = Repo.get!(Project, project_id)
    conn = Project.installation_connection(project.repo_xref, Repo)
    previous = project.source_compatibility || %{}
    # Ordinary heartbeat reconciliation handles missed events and restarts.
    if previous["checked_at"] == nil or Outbox.now() - previous["checked_at"] >= 300 or
         (previous["required_sha"] != nil and previous["required_sha"] != previous["base_sha"]) do
      with true <- enabled?(),
           "local" <- LocalGit.mode(project, BorsNG.ProjectConfig.branch(), conn),
           {:ok, %{commit: base}} <- GitHub.get_branch(conn, BorsNG.ProjectConfig.branch()) do
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
                not p.is_draft and p.into_branch == ^BorsNG.ProjectConfig.branch() and
                is_nil(p.bundle_id) and
                (p.id in subquery(active) or p.queue_pending or
                   p.merge_eligibility["eligible"] == true or
                   p.source_compatibility["repair_policy"] == "provisional-diagnostic-v1"),
            order_by: p.id
          )

        metadata =
          from(p in eligible,
            select: %{id: p.id, commit: p.commit, source_compatibility: p.source_compatibility}
          )

        # Compare decoded identities in Elixir to avoid SQL JSON string quoting.
        # Inspect bounded lightweight metadata and perform source work for only
        # up to four stale heads in one helper call before yielding the mutator lane.
        newer = Repo.all(from(p in metadata, where: p.id > ^cursor, limit: 16))

        page =
          newer ++
            Repo.all(
              from(p in metadata,
                where: p.id <= ^cursor,
                limit: ^(16 - length(newer))
              )
            )

        epoch = latest_warm_epoch(project_id)
        pending = page |> Enum.filter(&stale?(&1, base, epoch)) |> Enum.take(@background_heads)
        selected = Enum.map(pending, &Repo.get!(Patch, &1.id))
        first = List.first(pending)
        last = List.last(pending) || List.last(page)
        visited = if last, do: last.id, else: cursor
        more = if first, do: Enum.any?(newer, &(&1.id == first.id)), else: length(newer) == 16

        check_with_epoch(project, conn, base, selected, checker, epoch)

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
          "cursor" => visited,
          "checked_at" => if(more, do: Outbox.now() - 240, else: Outbox.now())
        }

        project |> Ecto.Changeset.change(source_compatibility: progress) |> Repo.update!()
      else
        _ -> :ok
      end
    end
  end

  def admit(%Project{} = project, conn, batch, links, base) do
    if project.name == BorsNG.ProjectConfig.repo() and enabled?() and
         batch.into_branch == BorsNG.ProjectConfig.branch() and
         LocalGit.mode(project, BorsNG.ProjectConfig.branch(), conn) == "local" do
      with {:ok, %{commit: ^base}} <- GitHub.get_branch(conn, BorsNG.ProjectConfig.branch()) do
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

  defp stale?(patch, base, epoch) do
    r = patch.source_compatibility || %{}

    r["projection_pending"] == true or r["base_sha"] != base or
      r["head_sha"] != patch.commit or r["policy"] != @policy or
      (r["status"] in ["conflict", "missing_source"] and is_nil(r["repair_head"]) and
         r["hold_head"] != patch.commit) or
      (r["status"] == "pass" and not is_nil(r["repair_head"]) and r["repair_base"] != base) or
      transient_retry?(r, epoch)
  end

  # Existing successful Git work is a retry hint, never validation authority.
  # This includes reconstruction publication; ordinary source checks and API
  # assembly events cannot repeatedly manufacture readiness hints.
  defp latest_warm_epoch(project_id) do
    Repo.one(
      from(e in CandidateEvent,
        join: c in Candidate,
        on: c.id == e.candidate_id,
        where:
          c.project_id == ^project_id and c.into_branch == ^BorsNG.ProjectConfig.branch() and
            e.source == "bors.local_git" and
            e.kind in ["local_assembled", "local_conflict", "staging_pushed"],
        order_by: [desc: e.id],
        limit: 1,
        select: e.id
      )
    ) || 0
  end

  defp transient_retry?(r, epoch) do
    previous = if is_integer(r["source_retry_epoch"]), do: r["source_retry_epoch"], else: 0

    transient =
      r["reason"] in ~w(source_helper_timeout source_helper_lock_timeout source_helper_cold_mirror_unavailable timeout lock_timeout) or
        (r["reason"] == "unknown_base_headers" and
           r["base_headers_reason"] in ~w(timeout lock_timeout))

    r["status"] == "unknown" and transient and epoch > previous and
      r["repair_head"] != r["head_sha"] and r["hold_head"] != r["head_sha"]
  end

  defp retry_context(results, epoch) do
    Enum.map(results, fn result ->
      if result["status"] == "unknown",
        do: Map.put(result, "source_retry_epoch", epoch),
        else: result
    end)
  end

  def requested?(patch, head),
    do: get_in(patch.source_compatibility || %{}, ["repair_head"]) == head

  defp blocked?(patch, head),
    do:
      requested?(patch, head) or get_in(patch.source_compatibility || %{}, ["hold_head"]) == head

  def check(project, conn, base, patches, checker \\ &LocalGit.compatibility/4)
  def check(_, _, _, [], _), do: :ok

  def check(project, conn, base, patches, checker),
    do: check_with_epoch(project, conn, base, patches, checker, latest_warm_epoch(project.id))

  defp check_with_epoch(_, _, _, [], _, _), do: :ok

  defp check_with_epoch(project, conn, base, patches, checker, epoch) do
    Enum.each(patches, fn patch ->
      if requested?(patch, patch.commit) and
           patch.source_compatibility["projection_pending"] == true,
         do: project_request(conn, patch)
    end)

    cached =
      Enum.filter(patches, fn patch ->
        r = patch.source_compatibility || %{}

        not BorsNG.Worker.BumpRecovery.automatic?(patch) and
          r["base_sha"] == base and r["head_sha"] == patch.commit and r["policy"] == @policy and
          ((r["status"] in ["conflict", "missing_source"] and r["hold_head"] == patch.commit) or
             (r["status"] == "pass" and requested?(patch, patch.commit) and
                r["repair_policy"] == @policy and r["repair_base"] != base))
      end)

    # Retry projections/marker resolution from exact cached evidence, not Git.
    # apply_results still verifies live main/head/withdrawals before any action.
    if cached != [],
      do: apply_results(project, conn, base, cached, Enum.map(cached, & &1.source_compatibility))

    pending =
      Enum.reject(patches, fn patch ->
        r = patch.source_compatibility || %{}

        BorsNG.Worker.BumpRecovery.automatic?(patch) or
          (r["base_sha"] == base and r["head_sha"] == patch.commit and r["policy"] == @policy and
             ((r["status"] not in ["conflict", "missing_source"] and
                 not transient_retry?(r, epoch)) or blocked?(patch, patch.commit)))
      end)

    # Independent helper requests keep inventory work bounded even when a
    # caller supplies an assembly-sized batch. Failures affect only their chunk.
    Enum.each(Enum.chunk_every(pending, 32), fn pending ->
      members = Enum.map(pending, &%{pr: &1.pr_xref, head_sha: &1.commit})

      case checker.(project, conn, base, members) do
        %{"status" => "ok", "results" => results} ->
          apply_results(project, conn, base, pending, retry_context(results, epoch))

        failure ->
          # A bounded fetch/lock/auth failure is unknown coverage, not a pass
          # and not an invisible completed sweep. Keep it exact-head/base bound
          # and leave authoritative staging available without repeated scans.
          reason =
            case failure do
              %{"details" => %{"kind" => kind}}
              when kind in ~w(timeout lock_timeout git_failed mirror_limit output_limit helper_failed git_auth_unavailable helper_exit invalid_helper_json helper_unavailable missing_commit cold_mirror_unavailable) ->
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

          details =
            case failure do
              %{"details" => details} when is_map(details) -> details
              _ -> %{}
            end

          phase = details["phase"]
          operation = details["operation"]

          results =
            Enum.map(results, fn result ->
              result =
                if phase in ~w(lock initialize fetch base_headers merge_tree candidate_headers),
                  do: Map.put(result, "phase", phase),
                  else: result

              if operation in ~w(init for-each-ref update-ref gc cat-file count-objects fetch ls-tree merge-tree),
                do: Map.put(result, "operation", operation),
                else: result
            end)

          apply_results(project, conn, base, pending, retry_context(results, epoch))
      end
    end)
  end

  @doc false
  def apply_results(project, conn, base, patches, results) do
    with {:ok, %{commit: ^base}} <- GitHub.get_branch(conn, BorsNG.ProjectConfig.branch()) do
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

        Map.drop(result, [
          "repair_head",
          "repair_base",
          "repair_policy",
          "projection_pending",
          "action"
        ])
      else
        _ ->
          Map.put(
            result,
            "action",
            "source compatible; request resolution incomplete; author/maintainer attention required"
          )
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
          Map.take(
            result,
            ~w(pr head_sha base_sha policy status reason coverage phase operation base_headers_reason base_headers_operation source_retry_epoch)
          )
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
      persist_request(project, conn, patch, result)

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

  # Database/outbox writes only; callers perform live guards before this step.
  defp persist_request(project, conn, patch, result) do
    result = Map.drop(result, ["hold_head", "action"])

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
    else
      patch |> Ecto.Changeset.change(source_compatibility: result) |> Repo.update!()
    end
  end

  defp repair_metadata(patch) do
    if requested?(patch, patch.commit),
      do:
        Map.take(
          patch.source_compatibility || %{},
          ~w(repair_head repair_base repair_policy projection_pending provisional)
        ),
      else: %{}
  end

  # Diagnostic owners are scheduling suspects, not individual build verdicts.
  # Only the failed-CI and construction-conflict split callsites can offer
  # this; generic bisection and local import hints continue through their
  # existing conservative path. Every suspect is withdrawn, the remainder is
  # admitted once and the parent is retired in one transaction, so a replay
  # cannot hand off twice or leave a suspect beside committed children.
  def provisional_failure(batch, links, suspects, remainder, hints, conn, conflict \\ nil) do
    project = Repo.get!(Project, batch.project_id)
    candidate = handoff_candidate(batch, conflict)
    patches = Enum.map(suspects, &Repo.get!(Patch, &1.patch_id))
    prs = Enum.sort(Enum.map(patches, & &1.pr_xref))

    with true <- enabled?(),
         true <- Outbox.enabled?(),
         true <- suspects != [],
         true <- project.name == BorsNG.ProjectConfig.repo(),
         true <- batch.into_branch == BorsNG.ProjectConfig.branch(),
         true <- BorsNG.Worker.FailureIsolation.ownership_complete?(hints),
         true <- not is_nil(candidate),
         true <- independent_provisional_base?(candidate, batch),
         true <- Enum.all?(patches, &(is_nil(&1.bundle_id) and is_nil(&1.manual_approval))),
         true <- Enum.all?(suspects, &is_nil(&1.manual_approval)),
         false <- Enum.any?(patches, &BorsNG.Worker.BumpRecovery.automatic?/1),
         true <-
           Enum.zip(patches, suspects)
           |> Enum.all?(fn {patch, link} ->
             patch.commit == link.head_sha and not requested?(patch, link.head_sha)
           end),
         false <- Outbox.withdrawal_pending?(project.id, Enum.map(links, & &1.patch_id)),
         true <-
           Enum.sort(Enum.map(links, &{&1.patch_id, &1.head_sha})) ==
             Enum.sort(Enum.map(candidate.snapshot["members"], &{&1["patch_id"], &1["head_sha"]})),
         {:ok, base, evidence} <- suspect_evidence(batch, candidate, hints, prs, conflict),
         {:ok, %{commit: ^base}} <- GitHub.get_branch(conn, BorsNG.ProjectConfig.branch()),
         true <-
           Enum.all?(Enum.zip(patches, suspects), fn {patch, link} ->
             match?(
               {:ok, %{head_sha: head, state: :open, draft: false}} when head == link.head_sha,
               GitHub.get_pr(conn, patch.pr_xref)
             )
           end),
         true <-
           Enum.all?(patches, fn patch ->
             case GitHub.get_labels(conn, patch.pr_xref) do
               {:ok, labels} ->
                 MapSet.disjoint?(MapSet.new(labels, &String.downcase/1), MapSet.new(@keep))

               _ ->
                 false
             end
           end) do
      results =
        Map.new(Enum.zip(patches, suspects), fn {patch, link} ->
          {patch.id, suspect_result(patch, link, batch, candidate, base, evidence, conflict)}
        end)

      expected_state = if conflict, do: [:waiting, :running], else: [:running]

      case Repo.transaction(fn ->
             parent = Repo.one!(from(b in Batch, where: b.id == ^batch.id, lock: "FOR UPDATE"))

             current =
               Enum.map(suspects, fn link ->
                 Repo.one!(from(p in Patch, where: p.id == ^link.patch_id, lock: "FOR UPDATE"))
               end)

             stale_suspect? =
               Enum.zip(current, suspects)
               |> Enum.any?(fn {patch, link} ->
                 patch.commit != link.head_sha or requested?(patch, link.head_sha) or
                   not is_nil(patch.manual_approval) or not is_nil(patch.bundle_id)
               end)

             if stale_suspect? or parent.state not in expected_state or
                  parent.commit != batch.commit or parent.base_sha != batch.base_sha or
                  not independent_provisional_base?(candidate, parent) or
                  handoff_candidate(parent, conflict) != candidate,
                do: Repo.rollback(:stale_provisional_binding)

             # Retire parent together with every remainder admission. A crash
             # cannot resurrect a suspect alongside already-committed children.
             Enum.each(remainder, fn group ->
               if group != [] and
                    is_nil(
                      BorsNG.Worker.Batcher.Divider.clone_batch(
                        group,
                        project.id,
                        BorsNG.ProjectConfig.branch(),
                        "provisional_remainder"
                      )
                    ),
                  do: Repo.rollback(:retry_membership_unavailable)
             end)

             Enum.each(Enum.zip(current, suspects), fn {patch, link} ->
               persist_request(project, conn, patch, results[patch.id])
               patch = Repo.get!(Patch, patch.id)
               unless requested?(patch, link.head_sha), do: Repo.rollback(:repair_unavailable)
               BorsNG.Worker.QueueApproval.stop([patch], :source_compatibility)
             end)

             parent |> Batch.changeset(%{state: :error}) |> Repo.update!()

             BorsNG.Worker.CandidateJournal.event(
               candidate,
               "provisional_repair",
               "bors.failure_isolation",
               %{
                 pr: hd(prs),
                 prs: prs,
                 # Each suspect's retained age lets its next different head
                 # keep this position once; see deploy/queue-age.md.
                 suspects:
                   Enum.map(suspects, fn link ->
                     Map.merge(
                       BorsNG.Worker.CandidateJournal.handoff_record(link),
                       %{"pr" => link.patch.pr_xref, "head_sha" => link.head_sha}
                     )
                   end),
                 head_sha: hd(suspects).head_sha,
                 base_sha: base,
                 binding: evidence["binding"],
                 conflict: if(conflict, do: Map.take(conflict, ~w(paths merge_head))),
                 remainder: Enum.map(remainder, &Enum.map(&1, fn l -> l.patch.pr_xref end)),
                 authority: "provisional_suspect",
                 proof_authority: false,
                 singleton_confirmation_dispatched: false
               }
             )
           end) do
        {:ok, _} ->
          Enum.each(patches, &project_request(conn, Repo.get!(Patch, &1.id)))
          :handed_off

        _ ->
          :unavailable
      end
    else
      _ -> :unavailable
    end
  end

  # A failed CI candidate is bound to the batch commit. A construction conflict
  # never bound one, so the latest generation of the same batch is the record.
  defp handoff_candidate(batch, nil), do: BorsNG.Worker.CandidateJournal.current(batch)
  defp handoff_candidate(batch, _conflict), do: BorsNG.Worker.CandidateJournal.latest(batch.id)

  # A failed candidate's bound diagnostics must name exactly these suspects.
  # A construction conflict has no CI binding; its local conflict event names
  # the single conflicting head and the exact base it was merged onto.
  defp suspect_evidence(batch, candidate, hints, prs, nil) do
    with {:ok, owners, diagnosis} <- BorsNG.Worker.FailureDiagnostics.diagnosis(candidate, hints),
         true <- Enum.sort(owners) == prs,
         true <-
           diagnosis["binding"]["commit"] == batch.commit and
             candidate.snapshot["base_sha"] == batch.base_sha,
         base when is_binary(base) <- batch.base_sha do
      {:ok, base, diagnosis}
    else
      _ -> :unavailable
    end
  end

  defp suspect_evidence(_batch, _candidate, _hints, prs, conflict) do
    with "merge_conflict" <- conflict["kind"],
         [pr] <- prs,
         true <- conflict["pr"] == pr,
         base when is_binary(base) <- conflict["base_sha"],
         paths when is_list(paths) and paths != [] <- conflict["paths"],
         true <- Enum.all?(paths, &is_binary/1) do
      {:ok, base, %{"binding" => nil, "paths" => paths, "merge_head" => conflict["merge_head"]}}
    else
      _ -> :unavailable
    end
  end

  defp suspect_result(patch, link, batch, candidate, base, evidence, nil) do
    %{
      "pr" => patch.pr_xref,
      "head_sha" => link.head_sha,
      "base_sha" => base,
      "policy" => @provisional_policy,
      "status" => "suspected_failure",
      "run_url" =>
        "https://github.com/#{BorsNG.ProjectConfig.repo()}/actions/runs/#{evidence["binding"]["run_id"]}",
      "provisional" =>
        Map.merge(evidence, %{
          "candidate_id" => candidate.id,
          "batch_id" => batch.id,
          "source_link_id" => link.id,
          "members" => candidate.snapshot["members"],
          "head_sha" => link.head_sha,
          "base_sha" => base,
          "requested_at" => Outbox.now(),
          "proof_authority" => false
        })
    }
  end

  # A batch-mate conflict uses the ordinary source policy: once main moves,
  # the existing sweep rechecks this head, and a clean merge resolves the
  # request without author work. A conflict with landed main stays requested.
  defp suspect_result(patch, link, batch, candidate, base, evidence, _conflict) do
    %{
      "pr" => patch.pr_xref,
      "head_sha" => link.head_sha,
      "base_sha" => base,
      "policy" => @policy,
      "status" => "conflict",
      "paths" => Enum.take(evidence["paths"], 64),
      "merge_head" => evidence["merge_head"],
      "provisional" => %{
        "candidate_id" => candidate.id,
        "batch_id" => batch.id,
        "source_link_id" => link.id,
        "head_sha" => link.head_sha,
        "base_sha" => base,
        "requested_at" => Outbox.now(),
        "proof_authority" => false
      }
    }
  end

  # A child becomes independent only after its exact predecessor actually lands
  # and its tested base is that landed commit. The live-main check above still
  # applies. Pending/failed/superseded ancestry cannot authorize individual blame.
  defp independent_provisional_base?(candidate, batch) do
    alias BorsNG.Worker.CandidateJournal

    case candidate.snapshot["speculative_predecessor_id"] do
      nil ->
        true

      id when is_integer(id) ->
        with predecessor when not is_nil(predecessor) <- Repo.get(BorsNG.Database.Candidate, id),
             parent when not is_nil(parent) <- Repo.get(Batch, predecessor.batch_id),
             true <- parent.state == :ok and parent.id != batch.id,
             latest when not is_nil(latest) <- CandidateJournal.latest(parent.id),
             true <- latest.id == id,
             true <- BorsNG.Worker.SpeculativeTrain.event?(predecessor, "promoted"),
             true <- CandidateJournal.commit_binding(predecessor)["commit"] == batch.base_sha,
             true <- parent.commit == batch.base_sha do
          true
        else
          _ -> false
        end

      _ ->
        false
    end
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
         true <- project.name == BorsNG.ProjectConfig.repo(),
         true <- batch.into_branch == BorsNG.ProjectConfig.branch(),
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
        "run_url" => "https://github.com/#{BorsNG.ProjectConfig.repo()}/actions/runs/#{run}"
      }

      patch = Repo.get!(Patch, link.patch_id)

      if patch.commit == link.head_sha do
        result =
          Map.merge(
            result,
            repair_metadata(patch)
          )

        result =
          if result["repair_policy"] == @provisional_policy do
            # An actual isolated failure is stronger evidence. Preserve original
            # lineage/consumed fallback, without renewing the head's repair task.
            marker = "<!--tauceti-provisional-outcome:v1 #{patch.commit} #{run}-->"

            Outbox.enqueue(
              project.id,
              "notice",
              {conn, patch.pr_xref,
               "This PR also failed when tested on its own against `main`. " <>
                 "The failure is now confirmed for this PR: [build log](#{result["run_url"]}).\n\n" <>
                 "Tested head: `#{patch.commit}`. Tested main: `#{batch.base_sha}`.\n\n#{marker}",
               marker},
              "provisional-outcome:#{project.id}:#{patch.pr_xref}:#{patch.commit}:#{run}"
            )

            result
            |> Map.put("repair_policy", "authoritative-individual-v1")
            |> Map.put("repair_base", batch.base_sha)
          else
            result
          end

        patch = patch |> Ecto.Changeset.change(source_compatibility: result) |> Repo.update!()
        handoff(project, conn, patch, result)
      end
    else
      _ -> :ok
    end
  end

  def request(head, result) do
    {summary, next_step} =
      case result["status"] do
        "suspected_failure" ->
          {"checking a possible build failure.",
           "Check this PR on its own against the tested main below. If it passes, report that the cause " <>
             "is still unconfirmed. If it fails, update the branch from `main`, fix the errors, and run " <>
             "the build and axiom checks before pushing."}

        "conflict" ->
          {"this branch conflicts with `main`.",
           "Update the branch from `main`, resolve the conflicts, and run the build and axiom checks before pushing."}

        "missing_source" ->
          {"this PR imports modules that are missing from `main`.",
           "Update the branch from `main`, fix the imports, and run the build and axiom checks before pushing."}

        "integrated_failure" ->
          {"this PR failed to build with `main`.",
           "Update the branch from `main`, fix the errors in the build log, and run the build and axiom checks before pushing."}
      end

    details =
      case result["status"] do
        "missing_source" ->
          Enum.map_join(Enum.take(result["missing_imports"], 8), "\n", fn finding ->
            "- `#{finding["path"]}` imports missing `#{finding["module"]}` (`#{finding["provider_path"]}`)."
          end)

        "conflict" ->
          "Conflicting files:\n\n" <>
            Enum.map_join(Enum.take(result["paths"], 8), "\n", &"- `#{&1}`") <>
            if(result["provisional"],
              do:
                "\n\nThe conflict appeared while combining this PR with other queued PRs. " <>
                  "Bors rechecks this head against `main` whenever `main` moves and lifts the request " <>
                  "on its own if the branch then merges cleanly.",
              else: ""
            )

        "integrated_failure" ->
          "Bors tested this PR on its own against `main`. [Build log](#{result["run_url"]})."

        "suspected_failure" ->
          provisional = result["provisional"]

          paths =
            provisional["diagnostics"]["jobs"]
            |> Enum.flat_map(& &1["observations"])
            |> Enum.flat_map(& &1["paths"])
            |> Enum.uniq()
            |> Enum.take(8)

          hop =
            provisional["diagnostics"]["jobs"]
            |> Enum.flat_map(& &1["observations"])
            |> Enum.map(& &1["attributed_via"])
            |> Enum.find(&is_map/1)
            |> case do
              %{"path" => via, "imports" => imports} when is_list(imports) ->
                "\n\nThe failure was reported in `#{via}`, which imports this PR's " <>
                  Enum.map_join(imports, ", ", &"`#{&1}`") <> "."

              _ ->
                ""
            end

          "A batch containing this PR failed, with errors in:\n\n" <>
            Enum.map_join(paths, "\n", &"- `#{&1}`") <>
            hop <>
            "\n\nThis PR has not been tested on its own, so we haven't confirmed whether it caused the failure. " <>
            "[Build log](#{result["run_url"]}).\n\n" <>
            "Bors is continuing with the other PRs while this one is checked."
      end

    evidence =
      if result["status"] == "suspected_failure" do
        provisional = result["provisional"]

        "\nFailed candidate #{provisional["candidate_id"]}, batch #{provisional["batch_id"]}, " <>
          "commit `#{provisional["binding"]["commit"]}`, run attempt #{provisional["binding"]["run_attempt"]}."
      else
        ""
      end

    "Merge-queue recovery for head `#{String.slice(head, 0, 7)}`: #{summary}\n\n" <>
      "#{details}\n\n#{next_step}\n\nAdd the `keep` label to pause automatic repair.\n\n" <>
      "<details>\n<summary>Test details</summary>\n\n" <>
      "Tested main: `#{result["base_sha"]}`. Checker policy: `#{result["policy"]}`.#{evidence}\n\n" <>
      "</details>\n\n<!--tauceti-rebase:v1 #{head}-->"
  end
end
