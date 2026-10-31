defmodule BorsNG.Worker.LocalGitIntegrationTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    CandidateEvent,
    Installation,
    LinkPatchBatch,
    Patch,
    Project,
    Repo,
    Status
  }

  alias BorsNG.Worker.{Batcher, CandidateAssembler, CandidateJournal, ManualApproval}
  alias BorsNG.GitHub
  @conn {{:installation, 91}, 14}
  @base String.duplicate("b", 40)
  @head String.duplicate("a", 40)
  @tree String.duplicate("c", 40)
  @commit String.duplicate("d", 40)

  setup do
    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        installation_id: inst.id,
        repo_xref: 14
      })

    batch = Repo.insert!(%Batch{Batch.new(project.id, "main") | last_polled: 0})
    approval = %{"actor_id" => 7, "head_sha" => @head, "merge_base_sha" => @base}

    patch =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: 1,
        commit: @head,
        into_branch: "main",
        manual_approval: approval
      })

    link =
      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          patch_id: patch.id,
          batch_id: batch.id,
          head_sha: @head,
          reviewer: "human",
          manual_approval: approval
        })
      )

    toml = "status = [\"build\"]\nuse_squash_merge = true\n"

    GitHub.ServerMock.put_state(%{
      @conn => %{
        local_git_mode: "local",
        backend_snapshot: {:ok, %{backend: "bors", github_count: 0}},
        branches: %{"main" => @base, "staging" => @base},
        commits: %{},
        comments: %{1 => []},
        statuses: %{},
        labels: %{1 => []},
        files: %{@base => %{"bors.toml" => toml}, "main" => %{"bors.toml" => toml}},
        pulls: %{
          1 => %GitHub.Pr{
            number: 1,
            base_repo_id: 14,
            title: "Title",
            body: "Body",
            state: :open,
            base_ref: "main",
            head_sha: @head,
            user: %GitHub.User{id: 6, login: "author"}
          }
        },
        pr_commits: %{
          1 => [
            %GitHub.Commit{
              sha: @head,
              author_name: "Author",
              author_email: "author@example.invalid"
            }
          ]
        }
      }
    })

    # Substitute only the process boundary. Real Git plumbing is independently
    # exercised by local_git_test.py, including transport and stale leases.
    dir = Path.join(System.tmp_dir!(), "bors-helper-test-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    python = System.find_executable("python3")

    helper = """
    #!#{python}
    import json,os,sys
    request=json.load(open(sys.argv[2]))
    kind=os.environ.get('BORS_TEST_HELPER_FAILURE','')
    if kind == request['operation']:
        print(json.dumps(dict(status='error',details=dict(kind='network_unavailable'))))
    elif request['operation'] in ['import_providers', 'bounded_import_providers']:
        print(json.dumps(dict(operation=request['operation'], token=os.environ.get('BORS_GIT_TOKEN'), changed_paths=request.get('changed_paths'))))
    elif request['operation']=='assemble':
        hints={}
        if os.environ.get('BORS_TEST_MISSING_IMPORT'):
            hints=dict(complete=True,changed_files=[dict(pr=m['pr'],head_sha=m['head_sha'],paths=['TauCeti/File'+str(m['pr'])+'.lean']) for m in request['members']],import_edges=[],missing_imports=[dict(pr=1,path='TauCeti/File1.lean',provider_path='TauCeti/Absent.lean')])
            hints.update(ownership_complete=True,dependencies_complete=True,provenance=request['identity'] | dict(commit='#{@commit}',tree='#{@tree}',members=[dict(pr=m['pr'],head_sha=m['head_sha']) for m in request['members']]))
        print(json.dumps(dict(status='ok',result=request['identity'] | dict(commit='#{@commit}',tree='#{@tree}'),diagnostic_hints=hints,commits=request['members'],git_version='test',mirror_bytes=1)))
    else:
        print(json.dumps(dict(status='ok')))
    """

    File.write!(Path.join(dir, "python3"), helper)
    File.chmod!(Path.join(dir, "python3"), 0o755)
    old_path = System.get_env("PATH")
    System.put_env("PATH", dir <> ":" <> old_path)

    on_exit(fn ->
      System.put_env("PATH", old_path)
      System.delete_env("BORS_TEST_HELPER_FAILURE")
      System.delete_env("BORS_TEST_MISSING_IMPORT")
      File.rm_rf!(dir)
    end)

    {:ok, project: project, batch: batch, patch: patch, link: link}
  end

  test "provider policies need no credentials and direct rollback preserves the old operation",
       ctx do
    previous = System.get_env("BORS_IMPORT_PROVIDER_POLICY")

    on_exit(fn ->
      if previous,
        do: System.put_env("BORS_IMPORT_PROVIDER_POLICY", previous),
        else: System.delete_env("BORS_IMPORT_PROVIDER_POLICY")
    end)

    System.delete_env("BORS_IMPORT_PROVIDER_POLICY")
    changed = [%{"paths" => ["TauCeti/Provider.lean", "README.md", "TauCeti/Provider.lean"]}]
    # nil connection cannot supply API/publication credentials.
    result =
      BorsNG.Worker.LocalGit.retry_providers(
        ctx.project,
        nil,
        %{},
        ["TauCeti/Root.lean"],
        changed
      )

    assert result["operation"] == "bounded_import_providers"
    assert result["token"] in [nil, ""]
    assert result["changed_paths"] == ["TauCeti/Provider.lean"]

    for policy <- ["direct", "unknown"] do
      System.put_env("BORS_IMPORT_PROVIDER_POLICY", policy)

      result =
        BorsNG.Worker.LocalGit.retry_providers(
          ctx.project,
          nil,
          %{},
          ["TauCeti/Root.lean"],
          changed
        )

      assert result["operation"] == "import_providers"
      assert result["token"] in [nil, ""]
      assert result["changed_paths"] == nil
    end
  end

  defp events(batch),
    do:
      Repo.all(
        from(e in CandidateEvent,
          where: e.candidate_id == ^CandidateJournal.latest(batch.id).id
        )
      )

  defp automatic_member(ctx) do
    Repo.delete!(ctx.link)
    patch = ctx.patch |> Patch.changeset(%{manual_approval: nil}) |> Repo.update!()

    link =
      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          batch_id: ctx.batch.id,
          patch_id: patch.id,
          head_sha: patch.commit,
          reviewer: "automatic reviewer"
        })
      )

    mock =
      GitHub.ServerMock.get_state()
      |> put_in([@conn, :admin_permissions], %{7 => {:ok, true}, 8 => {:ok, true}})
      |> put_in([@conn, :merge_bases], %{{1, @head} => {:ok, @base}})

    GitHub.ServerMock.put_state(mock)
    %{ctx | patch: patch, link: link}
  end

  defp scheduler_mode(mode, async, project_id) do
    mock =
      put_in(GitHub.ServerMock.get_state(), [@conn, :local_git_mode], mode)
      |> Map.put(:merge_tree, @tree)

    # Native serial merging reads a temporary-branch policy. Scoped main uses
    # the exact base policy; both retain squash construction in this fixture.
    GitHub.ServerMock.put_state(mock)

    if async do
      :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
      System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
      Process.put(:orchestration_executor, true)
      job = BorsNG.Worker.Orchestration.Outbox.enqueue(project_id, "info", {:poll, :once})
      {:ok, claimed} = BorsNG.Worker.Orchestration.Outbox.claim(job.id)
      Process.put(:orchestration_job, claimed)

      on_exit(fn ->
        System.delete_env("BORS_ASYNC_ORCHESTRATION")
        :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
      end)
    end
  end

  defp mixed_group(ctx) do
    bundle = Repo.insert!(BorsNG.Database.PatchBundle.new(ctx.project.id))
    patch = ctx.patch |> Patch.changeset(%{bundle_id: bundle.id}) |> Repo.update!()
    ctx = automatic_member(%{ctx | patch: patch})

    for number <- [2, 3] do
      sibling =
        Repo.insert!(%Patch{
          project_id: ctx.project.id,
          pr_xref: number,
          commit: @head,
          into_branch: "main",
          bundle_id: if(number == 2, do: bundle.id)
        })

      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          patch_id: sibling.id,
          batch_id: ctx.batch.id,
          head_sha: @head,
          reviewer: "sibling reviewer"
        })
      )

      mock = GitHub.ServerMock.get_state()

      mock
      |> put_in([@conn, :pulls, number], %{mock[@conn].pulls[1] | number: number})
      |> put_in([@conn, :pr_commits, number], mock[@conn].pr_commits[1])
      |> put_in([@conn, :comments, number], [])
      |> GitHub.ServerMock.put_state()
    end

    ctx.batch |> Batch.changeset(%{sealed: true}) |> Repo.update!()
    ctx
  end

  # Query telemetry runs in the scheduler caller after the INSERT. This places
  # the real mutation immediately after begin freezes its inputs, without a
  # production hook, sleep, mock approval or race-dependent timing.
  defp after_begin(action) do
    repo = :persistent_term.get(:db_repo)
    handler = make_ref()
    owner = self()

    :telemetry.attach(
      handler,
      repo.config()[:telemetry_prefix] ++ [:query],
      fn _, _, meta, _ ->
        if self() == owner and String.contains?(meta.query, "INSERT INTO") and
             String.contains?(meta.query, "candidate_events") and
             "construction_started" in meta.params do
          :telemetry.detach(handler)
          action.()
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp reconcile_construction(ctx, async \\ true) do
    candidate = CandidateJournal.latest(ctx.batch.id)

    if async do
      job =
        Repo.get_by!(BorsNG.Database.OrchestrationJob,
          dedup_key: "construction_reconcile:#{candidate.id}"
        )

      Batcher.handle_info(BorsNG.Worker.Orchestration.Outbox.payload(job), ctx.project.id)
      job
    else
      id = candidate.id
      assert_receive {:reconcile_construction, ^id, _} = message
      Batcher.handle_info(message, ctx.project.id)
    end
  end

  for mode <- ["off", "local"], async <- [false, true] do
    test "real queued takeover reaches running with frozen approval (#{mode}, async=#{async})",
         ctx do
      ctx = automatic_member(ctx)
      original = CandidateJournal.admission(ctx.link.id)
      scheduler_mode(unquote(mode), unquote(async), ctx.project.id)
      ctx.batch |> Batch.changeset(%{sealed: true}) |> Repo.update!()

      assert {:ok, approved} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)
      assert :ok == ManualApproval.preflight(@conn, approved)
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      batch = Repo.get!(Batch, ctx.batch.id)
      assert batch.state == :running and batch.sealed
      candidate = CandidateJournal.latest(batch.id)
      [member] = candidate.snapshot["members"]

      assert member ==
               original.snapshot
               |> Map.merge(%{
                 "admission_id" => original.id,
                 "link_id" => ctx.link.id,
                 "assembly_head_sha" => @head,
                 "manual_approval" => approved.manual_approval
               })

      assert CandidateJournal.admission(ctx.link.id) == original
      assert original.snapshot["manual_approval"] == nil
      binding = CandidateJournal.commit_binding(candidate)
      assert binding["commit"] == batch.commit

      assert binding["assembly_identity"]["members_digest"] ==
               CandidateJournal.digest(candidate.snapshot["members"])

      if unquote(async) do
        assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base

        job =
          Repo.get_by!(BorsNG.Database.OrchestrationJob,
            kind: "dispatch",
            project_id: ctx.project.id
          )

        {id, publisher} = BorsNG.Worker.Orchestration.Outbox.payload(job)
        Batcher.resume_dispatch(id, publisher)
      end

      if unquote(mode) == "off" do
        assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == batch.commit
      else
        # The local helper fixture substitutes transport and does not mutate
        # ServerMock; its successful leased publication is journaled instead.
        assert Enum.any?(events(batch), &(&1.kind == "staging_pushed"))
      end

      assert Enum.any?(events(batch), &(&1.kind == "dispatched"))
      Process.delete(:orchestration_executor)
    end

    test "post-begin takeover rejects then reconstructs once (#{mode}, async=#{async})", ctx do
      ctx = automatic_member(ctx)
      original = CandidateJournal.admission(ctx.link.id)
      scheduler_mode(unquote(mode), unquote(async), ctx.project.id)

      after_begin(fn ->
        assert {:ok, _} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)
      end)

      Batcher.handle_info({:poll, :once}, ctx.project.id)
      batch = Repo.get!(Batch, ctx.batch.id)
      first = CandidateJournal.latest(batch.id)
      assert batch.state == :waiting
      assert batch.recovery_after > BorsNG.Worker.Orchestration.Outbox.now()
      assert CandidateJournal.commit_binding(first) == %{}
      assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
      assert Enum.any?(events(batch), &(&1.kind == "construction_rejected"))
      refute BorsNG.Worker.Orchestration.Outbox.pending?(ctx.project.id, "dispatch:#{batch.id}")
      # The hold prevents immediate replay, then the normal scheduler builds a
      # new generation using the same admission and the current human decision.
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      assert CandidateJournal.latest(batch.id).id == first.id
      batch |> Batch.changeset(%{last_polled: 0, recovery_after: 0}) |> Repo.update!()
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      second = CandidateJournal.latest(batch.id)
      assert second.generation == first.generation + 1
      assert second.snapshot["previous_generation_id"] == first.id

      assert hd(second.snapshot["members"])["manual_approval"] ==
               Repo.get!(Patch, ctx.patch.id).manual_approval

      assert hd(first.snapshot["members"])["manual_approval"] == nil
      assert CandidateJournal.admission(ctx.link.id) == original
      assert Repo.get!(Batch, batch.id).state == :running

      result = CandidateAssembler.identity(first) |> Map.merge(%{commit: @commit, tree: @tree})
      assert {:error, :stale_candidate} == CandidateAssembler.validate_result(first, result)
      Process.delete(:orchestration_executor)
    end
  end

  test "mixed sealed bundle keeps every member and original admission through a real takeover",
       ctx do
    ctx = mixed_group(ctx)

    originals =
      Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))
      |> Map.new(&{&1.id, CandidateJournal.admission(&1.id)})

    scheduler_mode("local", true, ctx.project.id)
    assert {:ok, approved} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.sealed and batch.state == :running
    members = CandidateJournal.latest(batch.id).snapshot["members"]
    assert Enum.map(members, & &1["pr"]) == [1, 2, 3]
    assert Enum.map(members, & &1["manual_approval"]) == [approved.manual_approval, nil, nil]
    assert Enum.map(members, & &1["bundle_id"]) == [ctx.patch.bundle_id, ctx.patch.bundle_id, nil]

    for member <- members do
      assert CandidateJournal.admission(member["link_id"]) == originals[member["link_id"]]
    end

    Process.delete(:orchestration_executor)
  end

  test "revoking one member withdraws its whole bundle and lets an ordinary sibling recover",
       ctx do
    ctx = mixed_group(ctx)
    scheduler_mode("local", true, ctx.project.id)
    assert {:ok, _} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)

    originals =
      Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))
      |> Map.new(&{&1.id, CandidateJournal.admission(&1.id)})

    after_begin(fn -> ManualApproval.revoke(ctx.patch.id) end)
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    reconcile_construction(ctx)
    first = CandidateJournal.latest(ctx.batch.id)
    assert CandidateJournal.commit_binding(first) == %{}
    [kept] = Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))
    assert kept.patch.pr_xref == 3
    assert kept.manual_approval == nil
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :waiting and batch.sealed
    batch |> Batch.changeset(%{last_polled: 0, recovery_after: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    second = CandidateJournal.latest(ctx.batch.id)
    assert second.generation == first.generation + 1
    assert Enum.map(second.snapshot["members"], & &1["pr"]) == [3]
    assert Repo.get!(Batch, ctx.batch.id).state == :running
    assert length(first.snapshot["members"]) == 3
    for {id, original} <- originals, do: assert(CandidateJournal.admission(id) == original)
    Process.delete(:orchestration_executor)
  end

  test "unauthorized actor cannot replace queued authorization or admission", ctx do
    ctx = automatic_member(ctx)
    original = CandidateJournal.admission(ctx.link.id)

    assert {:error, :manual_approval_changed} ==
             ManualApproval.grant(@conn, ctx.project, ctx.patch, 999, @head)

    assert Repo.get!(LinkPatchBatch, ctx.link.id).manual_approval == nil
    assert Repo.get!(Patch, ctx.patch.id).manual_approval == nil
    assert CandidateJournal.admission(ctx.link.id) == original
  end

  test "durable executor constructs and dispatches a real queued takeover", ctx do
    ctx = automatic_member(ctx)
    scheduler_mode("local", true, ctx.project.id)
    original = CandidateJournal.admission(ctx.link.id)
    assert {:ok, approved} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)
    worker = start_supervised!({BorsNG.Worker.Orchestration.Worker, ctx.project.id})
    :sys.get_state(worker)
    poll = Process.get(:orchestration_job)
    send(worker, {:execute, self(), poll})
    assert_receive {:finished, ^worker, ^poll, :ok}, 5000
    BorsNG.Worker.Orchestration.Outbox.finish(poll)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running
    candidate = CandidateJournal.latest(batch.id)
    assert hd(candidate.snapshot["members"])["manual_approval"] == approved.manual_approval

    dispatch =
      Repo.get_by!(BorsNG.Database.OrchestrationJob, kind: "dispatch", project_id: ctx.project.id)

    {:ok, claimed} = BorsNG.Worker.Orchestration.Outbox.claim(dispatch.id)
    send(worker, {:execute, self(), claimed})
    assert_receive {:finished, ^worker, ^claimed, :ok}, 5000
    BorsNG.Worker.Orchestration.Outbox.finish(claimed)
    assert CandidateJournal.admission(ctx.link.id) == original
    assert Enum.any?(events(batch), &(&1.kind == "dispatched"))
    assert Process.alive?(worker)
    Process.delete(:orchestration_executor)
  end

  test "post-begin race preserves longer recovery and promotion holds", ctx do
    ctx = automatic_member(ctx)
    scheduler_mode("local", true, ctx.project.id)
    deadline = BorsNG.Worker.Orchestration.Outbox.now() + 900

    after_begin(fn ->
      assert {:ok, _} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)

      Repo.get!(Batch, ctx.batch.id)
      |> Batch.changeset(%{recovery_after: deadline, promotion_retry_at: deadline + 120})
      |> Repo.update!()
    end)

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :waiting
    assert batch.recovery_after == deadline
    assert batch.promotion_retry_at == deadline + 120
    candidate = CandidateJournal.latest(batch.id)

    timer =
      Repo.get_by!(BorsNG.Database.OrchestrationJob,
        dedup_key: "construction_retry:#{candidate.id}"
      )

    assert timer.available_at > batch.promotion_retry_at
    Process.delete(:orchestration_executor)
  end

  test "deferred reconciliation cannot revoke a later valid same-head grant", ctx do
    ctx = automatic_member(ctx)
    scheduler_mode("local", true, ctx.project.id)
    assert {:ok, _} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)
    after_begin(fn -> ManualApproval.revoke(ctx.patch.id) end)
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    first = CandidateJournal.latest(ctx.batch.id)

    assert {:ok, approved} =
             ManualApproval.grant(@conn, ctx.project, Repo.get!(Patch, ctx.patch.id), 8, @head)

    reconcile_construction(ctx)
    assert Repo.get!(Patch, ctx.patch.id).manual_approval == approved.manual_approval
    assert Repo.get!(LinkPatchBatch, ctx.link.id).manual_approval == approved.manual_approval
    batch = Repo.get!(Batch, ctx.batch.id)
    batch |> Batch.changeset(%{last_polled: 0, recovery_after: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :running
    assert CandidateJournal.latest(ctx.batch.id).generation == first.generation + 1
    Process.delete(:orchestration_executor)
  end

  test "deferred reconciliation respects newer intent and candidate generations", ctx do
    ctx = automatic_member(ctx)
    scheduler_mode("off", true, ctx.project.id)
    assert {:ok, _} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)
    after_begin(fn -> ManualApproval.revoke(ctx.patch.id) end)
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    first = CandidateJournal.latest(ctx.batch.id)

    Repo.get!(Patch, ctx.patch.id)
    |> Patch.changeset(%{orchestration_approval_token: "new-intent"})
    |> Repo.update!()

    job = reconcile_construction(ctx)
    assert Repo.get!(LinkPatchBatch, ctx.link.id)

    newer =
      CandidateJournal.begin(ctx.batch, Repo.all(LinkPatchBatch.from_batch(ctx.batch.id)), @base)

    Batcher.handle_info(BorsNG.Worker.Orchestration.Outbox.payload(job), ctx.project.id)
    assert CandidateJournal.latest(ctx.batch.id).id == newer.id
    assert newer.generation == first.generation + 1
    assert Repo.get!(LinkPatchBatch, ctx.link.id)
    assert Repo.get!(Patch, ctx.patch.id).orchestration_approval_token == "new-intent"
    Process.delete(:orchestration_executor)
  end

  test "ambiguous active parent-child overlap defers reconciliation without canceling either",
       ctx do
    ctx = automatic_member(ctx)
    scheduler_mode("off", true, ctx.project.id)
    assert {:ok, approved} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)

    after_begin(fn ->
      Repo.get!(LinkPatchBatch, ctx.link.id)
      |> LinkPatchBatch.changeset(%{reviewer: "other reviewer"})
      |> Repo.update!()
    end)

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    child = Repo.insert!(Batch.new(ctx.project.id, "main"))

    child_link =
      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          batch_id: child.id,
          patch_id: ctx.patch.id,
          head_sha: @head,
          reviewer: "automatic reviewer",
          manual_approval: approved.manual_approval
        }),
        ctx.link
      )

    job = reconcile_construction(ctx)
    assert Repo.get!(LinkPatchBatch, ctx.link.id)
    assert Repo.get!(LinkPatchBatch, child_link.id)
    assert Repo.get!(Patch, ctx.patch.id).manual_approval == approved.manual_approval
    child |> Batch.changeset(%{state: :error}) |> Repo.update!()
    Batcher.handle_info(BorsNG.Worker.Orchestration.Outbox.payload(job), ctx.project.id)
    refute Repo.get(LinkPatchBatch, ctx.link.id)
    assert Repo.get!(LinkPatchBatch, child_link.id)
    Process.delete(:orchestration_executor)
  end

  test "reconciliation revokes through the ordinary handler outside its rejection transaction",
       ctx do
    ctx = automatic_member(ctx)
    scheduler_mode("off", true, ctx.project.id)
    assert {:ok, _} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)

    after_begin(fn ->
      Repo.get!(LinkPatchBatch, ctx.link.id)
      |> LinkPatchBatch.changeset(%{reviewer: "other reviewer"})
      |> Repo.update!()
    end)

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    # Observe the actual revocation UPDATE. Its subsequent reset_scope request
    # runs in this same caller, after this standalone query has returned.
    owner = self()
    ref = make_ref()
    repo = :persistent_term.get(:db_repo)

    :telemetry.attach(
      ref,
      repo.config()[:telemetry_prefix] ++ [:query],
      fn _, _, meta, _ ->
        if self() == owner and String.starts_with?(meta.query, "UPDATE") and
             String.contains?(meta.query, "patches") and
             String.contains?(meta.query, "manual_approval"),
           do: send(owner, {:revocation_transaction, Repo.in_transaction?()})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(ref) end)
    reconcile_construction(ctx)
    assert_received {:revocation_transaction, false}
    refute_received {:revocation_transaction, true}
    refute Repo.get(LinkPatchBatch, ctx.link.id)
    Process.delete(:orchestration_executor)
  end

  for mutation <- [:revoke, :reviewer, :head, :remove], async <- [false, true] do
    test "post-begin #{mutation} fails closed without publication or permanent replay (async=#{async})",
         ctx do
      ctx = automatic_member(ctx)
      scheduler_mode("local", unquote(async), ctx.project.id)
      assert {:ok, _} = ManualApproval.grant(@conn, ctx.project, ctx.patch, 7, @head)

      after_begin(fn ->
        case unquote(mutation) do
          :revoke ->
            ManualApproval.revoke(ctx.patch.id)

          :reviewer ->
            Repo.get!(LinkPatchBatch, ctx.link.id)
            |> LinkPatchBatch.changeset(%{reviewer: "other reviewer"})
            |> Repo.update!()

          :head ->
            Repo.get!(Patch, ctx.patch.id)
            |> Patch.changeset(%{commit: @tree})
            |> Repo.update!()

          :remove ->
            Repo.delete!(ctx.link)
        end
      end)

      Batcher.handle_info({:poll, :once}, ctx.project.id)
      candidate = CandidateJournal.latest(ctx.batch.id)
      assert CandidateJournal.commit_binding(candidate) == %{}
      assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base

      refute BorsNG.Worker.Orchestration.Outbox.pending?(
               ctx.project.id,
               "dispatch:#{ctx.batch.id}"
             )

      if unquote(mutation) != :remove, do: reconcile_construction(ctx, unquote(async))
      refute Repo.exists?(LinkPatchBatch.from_batch(ctx.batch.id))
      Process.delete(:orchestration_executor)
    end
  end

  test "older rejected construction cannot overwrite a newer bound generation or statuses", ctx do
    ctx = automatic_member(ctx)
    scheduler_mode("off", true, ctx.project.id)

    after_begin(fn ->
      newer =
        CandidateJournal.begin(
          ctx.batch,
          Repo.all(LinkPatchBatch.from_batch(ctx.batch.id)),
          @base
        )

      :ok = CandidateJournal.bind(newer, @commit, @tree, @base, %Batcher.BorsToml{}, %{})

      Repo.get!(Batch, ctx.batch.id)
      |> Batch.changeset(%{
        state: :running,
        commit: @commit,
        recovery_after: 123,
        promotion_retry_at: 456
      })
      |> Repo.update!()

      Repo.insert!(%Status{batch_id: ctx.batch.id, identifier: "build", state: :error})
    end)

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running and batch.commit == @commit
    assert batch.recovery_after == 123 and batch.promotion_retry_at == 456
    assert Repo.one!(Status.all_for_batch(batch.id)).state == :error
    newer = CandidateJournal.latest(batch.id)
    assert newer.generation == 2
    assert CandidateJournal.commit_binding(newer)["commit"] == @commit
    assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
    refute BorsNG.Worker.Orchestration.Outbox.pending?(ctx.project.id, "dispatch:#{batch.id}")
    Process.delete(:orchestration_executor)
  end

  for {profile, email, name} <- [
        {nil, "6+author@users.noreply.github.com", "author"},
        {:error, "6+author@users.noreply.github.com", "author"},
        {%GitHub.FullUser{id: 99, login: "author", email: "other@example.invalid", name: "Other"},
         "6+author@users.noreply.github.com", "author"},
        {{:error, :read_transport, :timeout}, "6+author@users.noreply.github.com", "author"},
        {%GitHub.FullUser{id: 6, login: "author", email: nil, name: nil},
         "6+author@users.noreply.github.com", "author"},
        {%GitHub.FullUser{
           id: 6,
           login: "author",
           email: "public@example.invalid",
           name: "Public Name"
         }, "public@example.invalid", "Public Name"},
        {%GitHub.FullUser{id: 6, login: "author", email: "public@example.invalid", name: nil},
         "public@example.invalid", "author"}
      ] do
    test "async batched metadata preserves REST attribution (email=#{email}, name=#{name}, profile=#{inspect(profile)})",
         ctx do
      :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
      System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
      Process.put(:orchestration_executor, true)
      mock = GitHub.ServerMock.get_state()
      pr = get_in(mock, [@conn, :pulls, 1])

      commits =
        get_in(mock, [@conn, :pr_commits, 1]) ++
          [
            %GitHub.Commit{
              sha: @head,
              author_name: unquote(name),
              author_email: "same-name@example.invalid"
            },
            %GitHub.Commit{sha: @head, author_name: "Same Email", author_email: unquote(email)}
          ]

      data = %{pr: pr, commits: commits}

      mock =
        Map.put(mock, :batch_read_metadata, %{1 => data})
        |> Map.put(:users, %{"author" => unquote(Macro.escape(profile))})
        |> put_in([@conn, :pr_commits, 1], :invalid_if_rest_fallback)

      GitHub.ServerMock.put_state(mock)

      try do
        Batcher.handle_info({:poll, :once}, ctx.project.id)
        assert Repo.get!(Batch, ctx.batch.id).state == :running
        mock = GitHub.ServerMock.get_state()
        assert Enum.any?(mock.batch_read_calls, &(&1 == {:metadata, [1]}))
        assert Enum.any?(mock.batch_read_calls, &(&1 == {:identity, [1]}))
        assert mock.public_user_calls == ["author"]
        local = Enum.find(events(ctx.batch), &(&1.kind == "local_assembled"))
        [actual] = local.details["evidence"]["commits"]
        assert actual["committer"] == %{"name" => unquote(name), "email" => unquote(email)}

        expected =
          Batcher.Message.generate_squash_commit_message(
            pr,
            commits,
            unquote(email),
            unquote(name),
            nil
          )

        assert actual["commit_message"] == expected

        assert BorsNG.Worker.Orchestration.Outbox.pending?(
                 ctx.project.id,
                 "dispatch:#{ctx.batch.id}"
               )
      after
        Process.delete(:orchestration_executor)
        System.delete_env("BORS_ASYNC_ORCHESTRATION")
        :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
      end
    end
  end

  test "batched preparation reads a repeated author's public profile once", ctx do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    Process.put(:orchestration_executor, true)

    patch =
      Repo.insert!(%Patch{
        project_id: ctx.project.id,
        pr_xref: 2,
        commit: @head,
        into_branch: "main",
        manual_approval: ctx.patch.manual_approval
      })

    CandidateJournal.insert_member(
      LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
        batch_id: ctx.batch.id,
        patch_id: patch.id,
        head_sha: @head,
        reviewer: "human",
        manual_approval: patch.manual_approval
      })
    )

    mock = GitHub.ServerMock.get_state()
    pr1 = mock[@conn].pulls[1]
    pr2 = %{pr1 | number: 2}
    commits = mock[@conn].pr_commits[1]

    mock =
      put_in(mock, [@conn, :pulls, 2], pr2)
      |> Map.put(:batch_read_metadata, %{
        1 => %{pr: pr1, commits: commits},
        2 => %{pr: pr2, commits: commits}
      })

    GitHub.ServerMock.put_state(mock)

    try do
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      assert Repo.get!(Batch, ctx.batch.id).state == :running
      assert GitHub.ServerMock.get_state().public_user_calls == ["author"]
    after
      Process.delete(:orchestration_executor)
      System.delete_env("BORS_ASYNC_ORCHESTRATION")
      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end
  end

  test "unavailable batched metadata falls back to REST and dispatches",
       ctx do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    Process.put(:orchestration_executor, true)

    GitHub.ServerMock.put_state(
      Map.put(
        GitHub.ServerMock.get_state(),
        :batch_read_error,
        {:metadata, {:error, :unavailable}}
      )
    )

    try do
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      batch = Repo.get!(Batch, ctx.batch.id)
      assert batch.state == :running
      assert Enum.any?(events(batch), &(&1.kind == "constructed"))
      refute Enum.any?(events(batch), &(&1.kind == "failure_classified"))
      assert BorsNG.Worker.Orchestration.Outbox.pending?(ctx.project.id, "dispatch:#{batch.id}")
    after
      Process.delete(:orchestration_executor)
      System.delete_env("BORS_ASYNC_ORCHESTRATION")
      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end
  end

  test "unavailable REST metadata journals the affected PR and keeps its approval", ctx do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    Process.put(:orchestration_executor, true)
    mock = GitHub.ServerMock.get_state()
    mock = put_in(mock, [@conn, :pr_commits], %{})
    GitHub.ServerMock.put_state(mock)

    try do
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      assert Repo.get!(Batch, ctx.batch.id).state == :waiting
      error = Enum.find(events(ctx.batch), &(&1.kind == "local_error"))
      assert error.details["error"]["pr"] == 1
      assert error.details["error"]["cause"] =~ "get_pr_commits"
      assert Repo.get!(Patch, ctx.patch.id).manual_approval == ctx.patch.manual_approval
    after
      Process.delete(:orchestration_executor)
      System.delete_env("BORS_ASYNC_ORCHESTRATION")
      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end
  end

  test "unavailable required identity retains approvals and journals the specific cause", ctx do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    Process.put(:orchestration_executor, true)
    mock = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      Map.merge(mock, %{
        batch_read_error: {:identity, {:error, :batch_read, 502}},
        identity_read_error: {:error, :get_pr, 502, 1}
      })
    )

    try do
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      assert Repo.get!(Batch, ctx.batch.id).state == :waiting
      error = Enum.find(events(ctx.batch), &(&1.kind == "local_error"))
      assert error.details["error"]["pr"] == 1
      assert error.details["error"]["cause"] =~ "502"
      refute Enum.any?(events(ctx.batch), &(&1.kind == "failure_classified"))
      assert Repo.get!(Patch, ctx.patch.id).manual_approval == ctx.patch.manual_approval

      refute BorsNG.Worker.Orchestration.Outbox.pending?(
               ctx.project.id,
               "dispatch:#{ctx.batch.id}"
             )
    after
      Process.delete(:orchestration_executor)
      System.delete_env("BORS_ASYNC_ORCHESTRATION")
      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end
  end

  test "over-capacity existing batch is durably split before metadata or CI", ctx do
    System.put_env("BORS_SOURCE_COMPATIBILITY", "off")
    on_exit(fn -> System.delete_env("BORS_SOURCE_COMPATIBILITY") end)

    for n <- 2..129 do
      patch =
        Repo.insert!(%Patch{
          project_id: ctx.project.id,
          pr_xref: n,
          commit: @head,
          into_branch: "main",
          manual_approval: ctx.patch.manual_approval,
          priority: 7
        })

      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          batch_id: ctx.batch.id,
          patch_id: patch.id,
          head_sha: @head,
          reviewer: "human",
          manual_approval: patch.manual_approval
        })
      )
    end

    # The capacity check must precede per-member metadata, for which this mock
    # deliberately has no PRs beyond the original member.
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :error
    children = Repo.all(from(b in Batch, where: b.id != ^ctx.batch.id, order_by: b.id))
    assert length(children) == 2
    assert Enum.all?(children, &(&1.sealed and &1.priority == 7 and &1.state == :waiting))
    links = Enum.flat_map(children, &Repo.all(LinkPatchBatch.from_batch(&1.id)))
    assert length(links) == 129

    assert Enum.all?(
             links,
             &(&1.head_sha == @head and &1.manual_approval == ctx.link.manual_approval)
           )

    assert Enum.all?(
             links,
             &(CandidateJournal.admission(&1.id).snapshot["retry_reason"] == "bisection")
           )

    assert Enum.any?(events(ctx.batch), &(&1.kind == "local_capacity_partitioned"))
    refute Enum.any?(events(ctx.batch), &(&1.kind in ["local_assembled", "dispatch_requested"]))
    assert Repo.all(Status.all_for_batch(ctx.batch.id)) == []
  end

  test "over-capacity indivisible bundle stops without retrying forever", ctx do
    System.put_env("BORS_SOURCE_COMPATIBILITY", "off")
    on_exit(fn -> System.delete_env("BORS_SOURCE_COMPATIBILITY") end)

    for n <- 2..129 do
      patch =
        Repo.insert!(%Patch{
          project_id: ctx.project.id,
          pr_xref: n,
          commit: @head,
          into_branch: "main",
          manual_approval: ctx.patch.manual_approval,
          priority: 7
        })

      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          batch_id: ctx.batch.id,
          patch_id: patch.id,
          head_sha: @head,
          reviewer: "human",
          manual_approval: patch.manual_approval
        })
      )
    end

    bundle = Repo.insert!(BorsNG.Database.PatchBundle.new(ctx.project.id))

    Repo.update_all(from(p in Patch, where: p.project_id == ^ctx.project.id),
      set: [bundle_id: bundle.id, bundle_reviewer: "human"]
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :error
    assert Repo.aggregate(Batch, :count, :id) == 1
    assert Repo.get!(Patch, ctx.patch.id).queue_approval["reason"] == "member_limit"
    assert Repo.get!(Patch, ctx.patch.id).queue_approval["state"] == "failed"
    assert Repo.get!(Patch, ctx.patch.id).bundle_reviewer == nil
    assert Enum.any?(events(ctx.batch), &(&1.kind == "local_capacity_partitioned"))
    refute Enum.any?(events(ctx.batch), &(&1.kind in ["local_assembled", "dispatch_requested"]))
    assert Repo.all(Status.all_for_batch(ctx.batch.id)) == []
  end

  test "local result binds exact tree/base and authorizes the existing CI path", ctx do
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running
    assert batch.commit == @commit
    assert batch.base_sha == @base
    binding = CandidateJournal.commit_binding(CandidateJournal.latest(batch.id))
    assert binding["tree"] == @tree
    assert binding["base_sha"] == @base
    assert Repo.one!(Status.all_for_batch(batch.id)).state == :running
    assert Enum.any?(events(batch), &(&1.kind == "constructed" and &1.source == "bors.local_git"))
    assert Enum.any?(events(batch), &(&1.kind == "staging_pushed"))
    refute Map.has_key?(GitHub.ServerMock.get_state()[@conn].branches, "staging.tmp")
    assert Repo.get!(LinkPatchBatch, ctx.link.id).head_sha == @head
  end

  test "exact inventory partitions a missing import before CI, preserving approvals", ctx do
    System.put_env("BORS_TEST_MISSING_IMPORT", "true")

    patch =
      Repo.insert!(%Patch{
        project_id: ctx.project.id,
        pr_xref: 2,
        commit: @head,
        into_branch: "main",
        manual_approval: ctx.patch.manual_approval
      })

    CandidateJournal.insert_member(
      LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
        patch_id: patch.id,
        batch_id: ctx.batch.id,
        head_sha: @head,
        reviewer: "human",
        manual_approval: patch.manual_approval
      })
    )

    state = GitHub.ServerMock.get_state()

    state =
      state
      |> put_in([@conn, :pulls, 2], %{state[@conn].pulls[1] | number: 2})
      |> put_in([@conn, :pr_commits, 2], state[@conn].pr_commits[1])
      |> put_in([@conn, :labels, 2], [])
      |> put_in([@conn, :comments, 2], [])

    GitHub.ServerMock.put_state(state)
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :error
    assert batch.commit == nil
    assert Repo.all(Status.all_for_batch(batch.id)) == []
    assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
    assert Enum.any?(events(batch), &(&1.kind == "local_import_failure"))

    refute Enum.any?(
             events(batch),
             &(&1.kind in [
                 "constructed",
                 "staging_pushed",
                 "dispatch_requested",
                 "head_quarantined"
               ])
           )

    children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
    assert length(children) == 2
    assert Enum.all?(children, & &1.sealed)

    for child <- children do
      [link] = Repo.all(LinkPatchBatch.from_batch(child.id))
      assert link.manual_approval == ctx.link.manual_approval
      assert link.head_sha == @head
      assert link.reviewer == "human"
      assert CandidateJournal.admission(link.id).snapshot["retry_reason"] == "missing_import"
    end

    assert Repo.get!(Patch, ctx.patch.id).manual_approval == ctx.patch.manual_approval
  end

  test "async local publication retains its immutable recipe across executor restart", ctx do
    alias BorsNG.Database.OrchestrationJob
    alias BorsNG.Worker.Orchestration.{Outbox, Worker}
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")

    on_exit(fn ->
      System.delete_env("BORS_ASYNC_ORCHESTRATION")
      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end)

    worker = start_supervised!({Worker, ctx.project.id})
    job = Outbox.enqueue(ctx.project.id, "info", {:poll, :once})
    {:ok, claimed} = Outbox.claim(job.id)
    send(worker, {:execute, self(), claimed})
    assert_receive {:finished, ^worker, ^claimed, :ok}, 5000
    Outbox.finish(claimed)
    dispatch = Repo.get_by!(OrchestrationJob, project_id: ctx.project.id, kind: "dispatch")
    {id, publisher} = Outbox.payload(dispatch)
    assert id == ctx.batch.id
    assert publisher.kind == :local_git
    assert is_binary(publisher.context.date)
    assert publisher.result.commit == @commit
    assert CandidateJournal.latest(id).generation == 1
    refute Enum.any?(events(ctx.batch), &(&1.kind == "staging_pushed"))
    assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
    stop_supervised!(Worker)
    restarted = start_supervised!({Worker, ctx.project.id})
    {:ok, delivery} = Outbox.claim(dispatch.id)
    send(restarted, {:execute, self(), delivery})
    assert_receive {:finished, ^restarted, ^delivery, :ok}, 5000
    Outbox.finish(delivery)
    assert CandidateJournal.latest(id).generation == 1
    assert Enum.any?(events(ctx.batch), &(&1.kind == "staging_pushed"))
    assert Enum.any?(events(ctx.batch), &(&1.kind == "dispatched"))
  end

  test "cancelled CI without statuses releases staging and dispatches unrelated queued work",
       ctx do
    old = System.get_env("BORS_STAGE_DISPATCH_PROJECT")
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", ctx.project.name)

    on_exit(fn ->
      if old,
        do: System.put_env("BORS_STAGE_DISPATCH_PROJECT", old),
        else: System.delete_env("BORS_STAGE_DISPATCH_PROJECT")
    end)

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    running = Repo.get!(Batch, ctx.batch.id)
    assert running.state == :running
    assert running.timeout_at > DateTime.to_unix(DateTime.utc_now())
    assert Repo.one!(Status.all_for_batch(running.id)).state == :running

    waiting = Repo.insert!(%Batch{Batch.new(ctx.project.id, "main") | last_polled: 0})

    other =
      Repo.insert!(%Patch{
        project_id: ctx.project.id,
        pr_xref: 2,
        commit: @head,
        into_branch: "main",
        manual_approval: ctx.patch.manual_approval
      })

    CandidateJournal.insert_member(
      LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
        batch_id: waiting.id,
        patch_id: other.id,
        head_sha: @head,
        reviewer: "human",
        manual_approval: other.manual_approval
      })
    )

    run = %{
      "id" => 123,
      "run_attempt" => 1,
      "event" => "repository_dispatch",
      "path" => ".github/workflows/pr-build.yml",
      "display_title" => "bors branch=main batch=#{running.id} head=#{@commit} base=#{@base}",
      "status" => "completed",
      "conclusion" => "cancelled"
    }

    mock = GitHub.ServerMock.get_state()

    mock
    |> put_in([@conn, :pulls, 2], %{mock[@conn].pulls[1] | number: 2})
    |> put_in([@conn, :pr_commits, 2], mock[@conn].pr_commits[1])
    |> put_in([@conn, :comments, 2], [])
    |> put_in([@conn, :staging_runs], %{{@commit, running.id} => run})
    |> GitHub.ServerMock.put_state()

    running |> Batch.changeset(%{last_polled: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, running.id).state == :canceled
    assert Repo.get!(Batch, running.id).commit == nil
    assert Repo.get!(Patch, ctx.patch.id).manual_approval == ctx.patch.manual_approval

    [retry] = Repo.all(Batch.all_for_patch(ctx.patch.id, :incomplete))
    assert retry.sealed and retry.recovery_after > DateTime.to_unix(DateTime.utc_now())
    assert [link] = Repo.all(LinkPatchBatch.from_batch(retry.id))
    assert link.head_sha == @head and link.manual_approval == ctx.link.manual_approval

    # Normal polling starts eligible work even while the interrupted members back off.
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, waiting.id).state == :running
    assert Repo.get!(Batch, waiting.id).recovery_after == 0
    assert {@commit, waiting.id} in GitHub.ServerMock.get_state()[@conn].dispatches
    refute Enum.any?(events(running), &(&1.kind == "head_quarantined"))
  end

  test "running shadow checks GitHub tree once without replacing CI identity", ctx do
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    candidate = CandidateJournal.latest(batch.id)
    state = GitHub.ServerMock.get_state()
    state = put_in(state, [@conn, :local_git_mode], "shadow")
    state = put_in(state, [@conn, :commit_trees], %{@commit => @tree})
    state = put_in(state, [@conn, :statuses, @commit], %{"build" => :running})
    GitHub.ServerMock.put_state(state)
    batch |> Batch.changeset(%{last_polled: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, batch.id)
    assert batch.commit == @commit
    assert batch.state == :running
    assert CandidateJournal.latest(batch.id).id == candidate.id
    compared = Enum.filter(events(batch), &(&1.kind == "shadow_compared"))
    assert length(compared) == 1
    assert hd(compared).details["matches"]
    batch |> Batch.changeset(%{last_polled: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert length(Enum.filter(events(batch), &(&1.kind == "shadow_compared"))) == 1
  end

  test "failed assembly retains exact approvals and never arms or pushes CI", ctx do
    System.put_env("BORS_TEST_HELPER_FAILURE", "assemble")
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :waiting
    assert batch.commit == nil
    assert batch.recovery_after > DateTime.to_unix(DateTime.utc_now())
    assert Repo.all(Status.all_for_batch(batch.id)) == []
    assert Repo.get!(LinkPatchBatch, ctx.link.id).manual_approval == ctx.link.manual_approval
    assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
  end

  test "failed push retains intent and restart reconstructs a new generation", ctx do
    System.put_env("BORS_TEST_HELPER_FAILURE", "publish")
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    first = CandidateJournal.latest(batch.id)
    assert batch.state == :waiting
    assert batch.commit == nil
    assert Repo.one!(Status.all_for_batch(batch.id)).state == :running
    assert Enum.any?(events(batch), &(&1.kind == "local_push_error"))
    System.delete_env("BORS_TEST_HELPER_FAILURE")
    batch |> Batch.changeset(%{last_polled: 0, recovery_after: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, batch.id).state == :running
    assert CandidateJournal.latest(batch.id).generation == first.generation + 1
    assert Repo.get!(LinkPatchBatch, ctx.link.id).manual_approval == ctx.link.manual_approval
  end

  test "changed approved head cannot enter local assembly or staging", ctx do
    state = GitHub.ServerMock.get_state()
    pr = %{state[@conn].pulls[1] | head_sha: String.duplicate("e", 40)}
    GitHub.ServerMock.put_state(put_in(state, [@conn, :pulls, 1], pr))
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    refute batch.state == :running
    assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
    refute Enum.any?(events(batch), &(&1.kind == "local_assembled"))
  end
end
