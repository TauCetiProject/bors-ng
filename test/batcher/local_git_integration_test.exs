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

  alias BorsNG.Worker.{Batcher, CandidateJournal}
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

  for {profile, email, name} <- [
        {nil, "6+author@users.noreply.github.com", "author"},
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
    test "async batched metadata preserves REST attribution (email=#{email}, name=#{name}, missing=#{is_nil(profile)})",
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

  test "unavailable batched metadata stops construction without a dispatch or proof failure",
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
      assert batch.state == :waiting
      assert batch.recovery_after >= BorsNG.Worker.Orchestration.Outbox.now() + 290
      assert Enum.any?(events(batch), &(&1.kind == "construction_stopped"))
      refute Enum.any?(events(batch), &(&1.kind in ["constructed", "failure_classified"]))
      refute BorsNG.Worker.Orchestration.Outbox.pending?(ctx.project.id, "dispatch:#{batch.id}")
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
