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
  # Existing worker rounds default to 90 minutes. One ordinary reconciliation
  # grace avoids leaving an unconfirmed/no-op suspect permanently stranded.
  @provisional_grace 95 * 60

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

  def chunk(project_id, checker \\ &LocalGit.compatibility(&1, &2, &3, &4, 8, 30)) do
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
                   p.merge_eligibility["eligible"] == true or
                   p.source_compatibility["repair_policy"] == "provisional-diagnostic-v1"),
            order_by: p.id
          )

        metadata =
          from(p in eligible,
            select: %{id: p.id, commit: p.commit, source_compatibility: p.source_compatibility}
          )

        # Compare decoded identities in Elixir: SQL JSON string casts have
        # different quoting/collation semantics on PostgreSQL and MySQL 8.4.
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

        # At most one metadata-only recovery per fair page. This is not another
        # source check, worker task, or a barrier ahead of ordinary dispatch.
        if patch = Enum.find(page, &fallback_due?/1),
          do: provisional_fallback(project, conn, Repo.get!(Patch, patch.id), base)

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

          if blocked?(current, patch.commit) and not fallback_active?(current),
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

          if blocked?(current, link.head_sha) and not fallback_member?(current, batch, links) do
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
          c.project_id == ^project_id and c.into_branch == "main" and
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

  defp fallback_due?(patch) do
    r = patch.source_compatibility || %{}
    p = r["provisional"] || %{}

    r["repair_head"] == patch.commit and r["repair_policy"] == @provisional_policy and
      is_integer(p["requested_at"]) and Outbox.now() - p["requested_at"] >= @provisional_grace and
      is_nil(p["fallback_batch_id"])
  end

  defp fallback_member?(patch, batch, links) do
    r = patch.source_compatibility || %{}
    p = r["provisional"] || %{}

    requested?(patch, patch.commit) and r["repair_policy"] == @provisional_policy and
      p["fallback_batch_id"] == batch.id and length(links) == 1 and
      hd(links).head_sha == patch.commit and r["status"] not in ["conflict", "missing_source"]
  end

  defp fallback_active?(patch) do
    r = patch.source_compatibility || %{}
    id = get_in(r, ["provisional", "fallback_batch_id"])

    r["repair_policy"] == @provisional_policy and
      r["status"] not in ["conflict", "missing_source"] and not is_nil(id) and
      Repo.exists?(from(b in Batch, where: b.id == ^id and b.state in [:waiting, :running]))
  end

  # A timeout/no-op is NOT a validation receipt. Keep the request active for an
  # in-flight worker/CAS push, but permit one sealed authoritative singleton.
  # Its original admission carries approval/age/lineage; no replayed command or
  # new human approval is invented. Normal dispatch rechecks authorization.
  @doc false
  def provisional_fallback(project, conn, patch, base) do
    with true <- fallback_due?(patch),
         %{"state" => "withdrawn", "reason" => "source_compatibility", "head_sha" => head} <-
           patch.queue_approval,
         true <- head == patch.commit,
         nil <- patch.bundle_id,
         nil <- patch.manual_approval,
         false <- Outbox.withdrawal_pending?(project.id, [patch.id]),
         {:ok, %{commit: ^base}} <- GitHub.get_branch(conn, "main"),
         {:ok, pr} <- GitHub.get_pr(conn, patch.pr_xref),
         true <- pr.state == :open and not pr.draft and pr.head_sha == head,
         {:ok, labels} <- GitHub.get_labels(conn, patch.pr_xref),
         true <- MapSet.disjoint?(MapSet.new(labels, &String.downcase/1), MapSet.new(@keep)) do
      Repo.transaction(fn ->
        current = Repo.one!(from(p in Patch, where: p.id == ^patch.id, lock: "FOR UPDATE"))

        unless current.commit == head and current.queue_approval == patch.queue_approval and
                 fallback_due?(current),
               do: Repo.rollback(:stale_provisional_fallback)

        p = current.source_compatibility["provisional"]
        source = Repo.get!(LinkPatchBatch, p["source_link_id"]) |> Repo.preload(:patch)

        unless source.patch_id == current.id and source.head_sha == head and
                 is_nil(source.manual_approval),
               do: Repo.rollback(:invalid_provisional_admission)

        child =
          BorsNG.Worker.Batcher.Divider.clone_batch(
            [source],
            project.id,
            "main",
            "provisional_unconfirmed"
          )

        if is_nil(child), do: Repo.rollback(:retry_membership_unavailable)

        unless Repo.aggregate(
                 from(l in LinkPatchBatch, where: l.batch_id == ^child.id),
                 :count,
                 :id
               ) == 1,
               do: Repo.rollback(:non_singleton_provisional_fallback)

        # Divider seals retries: this head cannot rejoin the failed combination.
        r = put_in(current.source_compatibility, ["provisional", "fallback_batch_id"], child.id)
        current |> Ecto.Changeset.change(source_compatibility: r) |> Repo.update!()

        BorsNG.Worker.CandidateJournal.event(
          Repo.get!(Candidate, p["candidate_id"]),
          "provisional_unconfirmed",
          "bors.source",
          %{
            head_sha: head,
            evidence_base: p["base_sha"],
            fallback_base: base,
            fallback_batch_id: child.id,
            proof_authority: false,
            reason:
              "no changed-head repair observed within existing round plus grace; one isolated authoritative fallback"
          }
        )
      end)
    else
      _ -> :unavailable
    end
  end

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
              when kind in ~w(timeout lock_timeout git_failed mirror_limit output_limit helper_failed helper_or_auth_failed helper_unavailable missing_commit cold_mirror_unavailable) ->
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
          ~w(repair_head repair_base repair_policy projection_pending provisional)
        ),
      else: %{}
  end

  # A diagnostic owner is a scheduling suspect, not an individual build verdict.
  # Only the failed-CI split callsite can offer this; generic singleton bisection
  # and local import hints continue through their existing conservative path.
  def provisional_failure(batch, links, suspect, remainder, hints, conn) do
    project = Repo.get!(Project, batch.project_id)
    candidate = BorsNG.Worker.CandidateJournal.current(batch)
    patch = Repo.get!(Patch, suspect.patch_id)

    with true <- enabled?(),
         true <- Outbox.enabled?(),
         "TauCetiProject/TauCeti" <- project.name,
         "main" <- batch.into_branch,
         true <- hints["ownership_complete"] == true,
         true <- not is_nil(candidate),
         false <- BorsNG.Worker.SpeculativeTrain.tracked?(candidate),
         nil <- patch.bundle_id,
         nil <- patch.manual_approval,
         nil <- suspect.manual_approval,
         false <- BorsNG.Worker.BumpRecovery.automatic?(patch),
         false <- requested?(patch, suspect.head_sha),
         false <- Outbox.withdrawal_pending?(project.id, Enum.map(links, & &1.patch_id)),
         true <- patch.commit == suspect.head_sha,
         true <-
           Enum.sort(Enum.map(links, &{&1.patch_id, &1.head_sha})) ==
             Enum.sort(Enum.map(candidate.snapshot["members"], &{&1["patch_id"], &1["head_sha"]})),
         {:ok, [pr_number], diagnosis} <-
           BorsNG.Worker.FailureDiagnostics.diagnosis(candidate, hints),
         true <- pr_number == patch.pr_xref,
         true <-
           diagnosis["binding"]["commit"] == batch.commit and
             candidate.snapshot["base_sha"] == batch.base_sha,
         {:ok, %{commit: base}} <- GitHub.get_branch(conn, "main"),
         true <- base == batch.base_sha,
         {:ok, pr} <- GitHub.get_pr(conn, patch.pr_xref),
         true <- pr.head_sha == suspect.head_sha and pr.state == :open and not pr.draft,
         {:ok, labels} <- GitHub.get_labels(conn, patch.pr_xref),
         true <- MapSet.disjoint?(MapSet.new(labels, &String.downcase/1), MapSet.new(@keep)) do
      result = %{
        "pr" => patch.pr_xref,
        "head_sha" => suspect.head_sha,
        "base_sha" => batch.base_sha,
        "policy" => @provisional_policy,
        "status" => "suspected_failure",
        "run_url" =>
          "https://github.com/TauCetiProject/TauCeti/actions/runs/#{diagnosis["binding"]["run_id"]}",
        "provisional" =>
          Map.merge(diagnosis, %{
            "candidate_id" => candidate.id,
            "batch_id" => batch.id,
            "source_link_id" => suspect.id,
            "members" => candidate.snapshot["members"],
            "head_sha" => suspect.head_sha,
            "base_sha" => batch.base_sha,
            "requested_at" => Outbox.now(),
            "proof_authority" => false
          })
      }

      case Repo.transaction(fn ->
             parent = Repo.one!(from(b in Batch, where: b.id == ^batch.id, lock: "FOR UPDATE"))
             current = Repo.one!(from(p in Patch, where: p.id == ^patch.id, lock: "FOR UPDATE"))

             if current.commit != suspect.head_sha or parent.state != :running,
               do: Repo.rollback(:stale_provisional_binding)

             # Retire parent together with every remainder admission. A crash
             # cannot resurrect its suspect alongside already-committed children.
             Enum.each(remainder, fn group ->
               if is_nil(
                    BorsNG.Worker.Batcher.Divider.clone_batch(
                      group,
                      project.id,
                      "main",
                      "provisional_remainder"
                    )
                  ),
                  do: Repo.rollback(:retry_membership_unavailable)
             end)

             handoff(project, conn, current, result)
             current = Repo.get!(Patch, current.id)
             unless requested?(current, suspect.head_sha), do: Repo.rollback(:repair_unavailable)
             BorsNG.Worker.QueueApproval.stop([current], :source_compatibility)
             parent |> Batch.changeset(%{state: :error}) |> Repo.update!()

             BorsNG.Worker.CandidateJournal.event(
               candidate,
               "provisional_repair",
               "bors.failure_isolation",
               %{
                 pr: patch.pr_xref,
                 head_sha: suspect.head_sha,
                 base_sha: batch.base_sha,
                 binding: diagnosis["binding"],
                 remainder: Enum.map(remainder, &Enum.map(&1, fn l -> l.patch.pr_xref end)),
                 authority: "provisional_suspect",
                 proof_authority: false,
                 singleton_confirmation_dispatched: false
               }
             )
           end) do
        {:ok, _} -> :handed_off
        _ -> :unavailable
      end
    else
      _ -> :unavailable
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

        result =
          if result["repair_policy"] == @provisional_policy do
            # An actual isolated failure is stronger evidence. Preserve original
            # lineage/consumed fallback, without renewing the head's repair task.
            marker = "<!--tauceti-provisional-outcome:v1 #{patch.commit} #{run}-->"

            Outbox.enqueue(
              project.id,
              "notice",
              {conn, patch.pr_xref,
               "The isolated follow-up for head `#{patch.commit}` at main `#{batch.base_sha}` " <>
                 "now failed authoritative CI: #{result["run_url"]}. The earlier combination " <>
                 "attribution was provisional; this is individual evidence. Existing repair attempt " <>
                 "limits remain unchanged.\n\n#{marker}", marker},
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

        "suspected_failure" ->
          provisional = result["provisional"]

          paths =
            provisional["diagnostics"]["jobs"]
            |> Enum.flat_map(& &1["observations"])
            |> Enum.flat_map(& &1["paths"])
            |> Enum.uniq()
            |> Enum.take(8)

          "PROVISIONAL SUSPECT, not a proven individual failure. Complete candidate-bound diagnostics " <>
            "mapped to this one unbundled PR in failed combination candidate #{provisional["candidate_id"]}, " <>
            "batch #{provisional["batch_id"]}, commit `#{provisional["binding"]["commit"]}`, " <>
            "run attempt #{provisional["binding"]["run_attempt"]}: #{result["run_url"]}.\n" <>
            "Diagnostic paths: #{Enum.join(paths, ", ")}. The remainder proceeds without this exact head; " <>
            "a separate bors singleton confirmation was skipped. An interaction may be responsible. " <>
            "Reproduce against the fixed evidence main before claiming repair; if this source passes alone, " <>
            "report attribution unconfirmed and do not manufacture a source change or call the PR broken. " <>
            "If no changed head appears within the existing 90-minute round plus 5-minute reconciliation grace, " <>
            "bors may try this unchanged head once in a sealed isolated candidate, retaining this request. " <>
            "That fallback is validation, not a repair verdict or another worker attempt."
      end

    summary =
      if result["status"] == "suspected_failure",
        do: "provisional integration repair requested; individual failure unproven.",
        else: "exact individual source compatibility failed."

    "Merge-queue recovery for head `#{String.slice(head, 0, 7)}`: #{summary}\n\n" <>
      "Tested main: `#{result["base_sha"]}`. Checker policy: `#{result["policy"]}`.\n#{details}\n\n" <>
      "The prospective tree includes this PR's added modules. This does not change its existing head-CI build status. " <>
      "Its author or tending worker should integrate one current main snapshot, repair imports/APIs/proofs as necessary, " <>
      "and pass build and axiom checks before a compare-and-swap safe push. Git rebase alone is not proof of repair. " <>
      "Normal fresh-head CI and review then apply. Existing ownership/fork/human-path restrictions, claims and per-PR retry caps apply; " <>
      "untended or unwritable heads require their author/maintainer. Add keep to pause.\n\n<!--tauceti-rebase:v1 #{head}-->"
  end
end
