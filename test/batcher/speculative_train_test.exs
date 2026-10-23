defmodule BorsNG.Worker.SpeculativeTrainTest do
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

  alias BorsNG.Worker.{Batcher, CandidateJournal, QueueApproval}
  alias BorsNG.Worker.SpeculativeTrain, as: Train
  alias BorsNG.Worker.Orchestration.Outbox
  alias BorsNG.Worker.Batcher.BorsToml
  alias BorsNG.GitHub
  @conn {{:installation, 91}, 14}
  @base String.duplicate("b", 40)
  @toml "status = [\"build\"]\nuse_squash_merge = true\n"
  @toolchain "leanprover/lean4:v4.35.0\n"

  setup do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    installation = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        installation_id: installation.id,
        repo_xref: 14
      })

    GitHub.ServerMock.put_state(%{
      @conn => %{
        branches: %{"main" => @base, "staging" => @base},
        commits: %{},
        comments: %{},
        statuses: %{},
        files: %{@base => %{"bors.toml" => @toml, "lean-toolchain" => @toolchain}},
        pulls: %{},
        pr_files: %{},
        labels: %{}
      }
    })

    Process.put(:orchestration_executor, true)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", project.name)

    on_exit(fn ->
      for name <-
            ["BORS_ASYNC_ORCHESTRATION", "BORS_STAGE_DISPATCH_PROJECT", "BORS_SPECULATIVE_TRAINS"] ++
              Enum.map([16231, 16232, 16234], &"BORS_SPECULATION_GATE_#{&1}"),
          do: System.delete_env(name)

      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end)

    %{project: project}
  end

  defp enable do
    System.put_env("BORS_SPECULATIVE_TRAINS", "true")

    for issue <- [16231, 16232, 16234],
        do:
          System.put_env(
            "BORS_SPECULATION_GATE_#{issue}",
            "https://github.com/TauCetiProject/TauCeti/issues/#{issue}#issuecomment-123"
          )
  end

  defp candidate(project, pr, base \\ @base, parent \\ nil, count \\ 1) do
    batch =
      Repo.insert!(%Batch{
        Batch.new(project.id, "main")
        | sealed: true,
          last_polled: 0,
          timeout_at: System.system_time(:second) + 3600
      })

    links =
      for n <- pr..(pr + count - 1) do
        head = String.pad_leading(Integer.to_string(n, 16), 40, "0")

        patch =
          Repo.insert!(%Patch{
            project_id: project.id,
            pr_xref: n,
            into_branch: "main",
            commit: head,
            queue_pending: true
          })

        patch = QueueApproval.remember(patch, "reviewer")
        state = GitHub.ServerMock.get_state()

        GitHub.ServerMock.put_state(
          state
          |> put_in([@conn, :pulls, n], %GitHub.Pr{
            number: n,
            head_sha: head,
            state: :open,
            base_ref: "main",
            title: "Test"
          })
          |> put_in([@conn, :pr_files, n], ["TauCeti/Test#{n}.lean"])
          |> put_in([@conn, :comments, n], [])
          |> put_in([@conn, :labels, n], [])
        )

        CandidateJournal.insert_member(
          LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
            batch_id: batch.id,
            patch_id: patch.id,
            head_sha: head,
            reviewer: "reviewer"
          })
        )
      end

    c =
      CandidateJournal.begin(
        batch,
        Repo.all(LinkPatchBatch.from_batch(batch.id)),
        base,
        if(parent, do: parent.id)
      )

    {:ok, toml} = BorsToml.new(@toml)
    head = String.pad_leading(Integer.to_string(1000 + pr, 16), 40, "0")

    CandidateJournal.bind(c, head, "tree", base, toml, %{
      content: @toolchain,
      sha256: CandidateJournal.digest(@toolchain)
    })

    CandidateJournal.event(c, "train_barrier_checked", "bors.local_git", %{
      commit: head,
      tree: "tree",
      base_sha: base,
      evidence: %{complete: true, paths: ["TauCeti/Test.lean"]}
    })

    batch =
      batch
      |> Batch.changeset(%{base_sha: base, commit: head, state: :running})
      |> Repo.update!()
      |> Repo.preload(:project)

    Repo.insert!(%Status{batch_id: batch.id, identifier: "build", state: :running})

    GitHub.ServerMock.put_state(
      GitHub.ServerMock.get_state()
      |> put_in([@conn, :files, head], %{"bors.toml" => @toml, "lean-toolchain" => @toolchain})
    )

    {batch, c, links}
  end

  defp main(head),
    do:
      GitHub.ServerMock.put_state(
        put_in(GitHub.ServerMock.get_state(), [@conn, :branches, "main"], head)
      )

  defp landed(batch, c) do
    batch |> Batch.changeset(%{state: :ok}) |> Repo.update!()
    CandidateJournal.event(c, "promoted", "test", %{commit: batch.commit})
    main(batch.commit)
  end

  defp run(c, status, conclusion \\ nil) do
    head = CandidateJournal.commit_binding(c)["commit"]
    state = GitHub.ServerMock.get_state()

    repo =
      Map.update(
        state[@conn],
        :staging_runs,
        %{{head, c.batch_id} => %{"status" => status, "conclusion" => conclusion}},
        &Map.put(&1, {head, c.batch_id}, %{"status" => status, "conclusion" => conclusion})
      )

    GitHub.ServerMock.put_state(Map.put(state, @conn, repo))
  end

  test "disabled by default, scoped and gated by all B/E/F evidence", %{project: project} do
    refute Train.enabled?(project, "main")
    enable()
    assert Train.enabled?(project, "main")
    refute Train.enabled?(project, "pilot")

    for issue <- [16231, 16232, 16234] do
      key = "BORS_SPECULATION_GATE_#{issue}"
      saved = System.get_env(key)
      System.delete_env(key)
      refute Train.enabled?(project, "main")
      System.put_env(key, saved)
    end
  end

  test "one successor admission per generation and no depth three", %{project: project} do
    enable()
    {a, ca, _} = candidate(project, 1)
    Train.reserve(ca, @conn)
    assert Train.predecessor(project, [a], @conn).id == ca.id
    {b, cb, _} = candidate(project, 2, a.commit, ca)
    assert cb.snapshot["base_sha"] == a.commit
    assert cb.snapshot["speculative_predecessor_id"] == ca.id
    assert is_nil(Train.predecessor(project, [a, b], @conn))
    CandidateJournal.event(ca, "successor_reserved", "test")
    assert is_nil(Train.predecessor(project, [a], @conn))
  end

  test "out-of-order green cannot promote B; exact A landing unlocks it after restart/disable", %{
    project: project
  } do
    {a, ca, _} = candidate(project, 1)
    {b, cb, _} = candidate(project, 2, a.commit, ca)
    assert {:ok, :ok} = Train.reserve(cb, @conn)
    assert Train.guard(b, @conn) == {:hold, :predecessor_running}
    # Exercise real completion intake, with all statuses green and no dispatch
    # pending: this must still hold, without consulting the mutable staging ref.
    GitHub.ServerMock.put_state(
      put_in(GitHub.ServerMock.get_state(), [@conn, :statuses, b.commit], %{"build" => :ok})
    )

    Batcher.do_handle_cast({:status, {b.commit, "build", :ok, "url"}}, project.id)
    assert Repo.get!(Batch, b.id).state == :running
    refute Train.event?(cb, "promoted")
    landed(a, ca)
    System.put_env("BORS_SPECULATIVE_TRAINS", "false")
    assert Train.guard(Repo.get!(Batch, b.id), @conn) == :ok
    assert Train.guard(%{b | base_sha: @base}, @conn) == {:invalidate, :base_mismatch}
    main("unexpected")
    assert Train.guard(b, @conn) == {:invalidate, :main_advanced}
  end

  test "parent failure invalidates and requeues B once without losing bundle/approval", %{
    project: project
  } do
    {a, ca, _} = candidate(project, 1)
    {b, cb, links} = candidate(project, 2, a.commit, ca, 2)
    bundle = Repo.insert!(%BorsNG.Database.PatchBundle{project_id: project.id})

    for link <- links,
        do:
          Repo.get!(Patch, link.patch_id)
          |> Ecto.Changeset.change(bundle_id: bundle.id)
          |> Repo.update!()

    before =
      Enum.map(
        Repo.all(LinkPatchBatch.from_batch(b.id)),
        &Map.take(&1, [:patch_id, :head_sha, :reviewer, :manual_approval])
      )

    a |> Batch.changeset(%{state: :error}) |> Repo.update!()
    assert Train.guard(b, @conn) == {:invalidate, :predecessor_invalid}
    Train.invalidate_descendants(a)
    Train.invalidate_descendants(a)
    assert Repo.get!(Batch, b.id).state == :canceled

    retry =
      Repo.one!(from(x in Batch, where: x.project_id == ^project.id and x.state == ^:waiting))

    assert retry.sealed

    assert Enum.map(
             Repo.all(LinkPatchBatch.from_batch(retry.id)),
             &Map.take(&1, [:patch_id, :head_sha, :reviewer, :manual_approval])
           ) == before

    assert Enum.count(
             Repo.all(
               from(e in CandidateEvent,
                 where: e.candidate_id == ^cb.id and e.kind == "train_invalidated"
               )
             )
           ) == 1
  end

  test "withdrawal and changed parent generation fence descendants", %{project: project} do
    {a, ca, [link]} = candidate(project, 1)
    {b, _, _} = candidate(project, 2, a.commit, ca)
    Outbox.enqueue(project.id, "cast", {:cancel, link.patch_id, :requested})
    assert Train.guard(b, @conn) == {:hold, :withdrawal_pending}
    Repo.get!(Patch, link.patch_id) |> Patch.changeset(%{commit: "new"}) |> Repo.update!()
    assert Train.guard(b, @conn) == {:invalidate, :predecessor_invalid}
  end

  test "unexpected main advance invalidates the whole train and preserves intent", %{
    project: project
  } do
    {a, ca, _} = candidate(project, 1)
    {b, _, _} = candidate(project, 2, a.commit, ca)
    Train.reserve(ca, @conn)
    main("out-of-band")
    assert Train.guard(a, @conn) == {:invalidate, :main_advanced}
    Train.invalidate(a, :main_advanced)
    assert Repo.get!(Batch, b.id).state == :canceled

    assert Repo.aggregate(
             from(x in Batch, where: x.project_id == ^project.id and x.state == ^:waiting),
             :count
           ) == 2
  end

  test "canceled jobs retain capacity until terminal exact workflow confirmation", %{
    project: project
  } do
    {a, ca, _} = candidate(project, 1)
    {b, cb, _} = candidate(project, 2)
    {c, cc, _} = candidate(project, 3)
    {_, cd, _} = candidate(project, 4)

    for x <- [ca, cb, cc] do
      assert {:ok, :ok} = Train.reserve(x, @conn)
      CandidateJournal.event(x, "dispatch_requested", "test")
      run(x, "in_progress")
    end

    for x <- [a, b, c], do: x |> Batch.changeset(%{state: :canceled}) |> Repo.update!()
    assert {:error, :staging_cap} = Train.reserve(cd, @conn)
    assert length(Train.reservations(project.id)) == 3
    run(ca, "completed", "cancelled")
    assert {:ok, :ok} = Train.reserve(cd, @conn)
    assert length(Train.reservations(project.id)) == 3
  end

  test "pin/policy barriers and unknown toolchain prevent speculative admission", %{
    project: project
  } do
    enable()
    {a, ca, _} = candidate(project, 1)

    for path <- [
          "lean-toolchain",
          "lake-manifest.json",
          "lakefile.lean",
          "bors.toml",
          ".github/bors.toml",
          "scripts/check.sh"
        ] do
      GitHub.ServerMock.put_state(
        put_in(GitHub.ServerMock.get_state(), [@conn, :pr_files, 1], [path])
      )

      assert is_nil(Train.predecessor(project, [a], @conn)), path
    end

    GitHub.ServerMock.put_state(
      GitHub.ServerMock.get_state()
      |> put_in([@conn, :pr_files, 1], ["TauCeti/A.lean"])
      |> put_in([@conn, :files, @base, "lean-toolchain"], "different")
    )

    refute Train.barrier_free?(ca, @conn)
  end

  test "scheduler constructs and dispatches only B on the immutable A base", %{project: project} do
    enable()
    {a, ca, _} = candidate(project, 1)
    Train.reserve(ca, @conn)
    {b, _, _} = candidate(project, 2)

    b
    |> Batch.changeset(%{state: :waiting, commit: nil, timeout_at: 0, last_polled: 0})
    |> Repo.update!()

    Repo.delete_all(Status.all_for_batch(b.id))
    state = GitHub.ServerMock.get_state()
    pr = %{state[@conn].pulls[2] | user: %GitHub.User{id: 6, login: "author"}}

    GitHub.ServerMock.put_state(
      state
      |> put_in([@conn, :pulls, 2], pr)
      |> put_in([@conn, :pr_commits], %{
        2 => [
          %GitHub.Commit{sha: pr.head_sha, author_name: "A", author_email: "a@example.invalid"}
        ]
      })
      |> put_in([@conn, :statuses, a.commit], %{"build" => :running})
      |> put_in([@conn, :local_git_mode], "local")
      |> put_in([@conn, :backend_snapshot], {:ok, %{backend: "bors", github_count: 0}})
    )

    helper = Path.join(System.tmp_dir!(), "bors-train-test-#{System.unique_integer([:positive])}")
    File.mkdir_p!(helper)
    python = System.find_executable("python3")

    File.write!(Path.join(helper, "python3"), """
    #!#{python}
    import json,sys
    r=json.load(open(sys.argv[2]))
    if r['operation']=='assemble':
        i=r['identity']
        print(json.dumps(dict(status='ok', result=i | dict(commit=f"{i['candidate_id']:040x}",tree='tree'), barriers=dict(complete=True,paths=['TauCeti/Test2.lean']), toolchain=dict(content='leanprover/lean4:v4.35.0' + chr(10)))))
    else:
        print(json.dumps(dict(status='ok')))
    """)

    File.chmod!(Path.join(helper, "python3"), 0o755)
    path = System.get_env("PATH")
    System.put_env("PATH", helper <> ":" <> path)

    on_exit(fn ->
      System.put_env("PATH", path)
      File.rm_rf!(helper)
    end)

    Batcher.handle_info({:poll, :once}, project.id)
    b = Repo.get!(Batch, b.id)
    cb = CandidateJournal.current(b)
    assert b.state == :running
    assert b.base_sha == a.commit
    assert cb.snapshot["speculative_predecessor_id"] == ca.id
    assert Enum.map(cb.snapshot["members"], & &1["pr"]) == [2]
    assert cb.generation == 2
    assert Train.event?(ca, "successor_reserved")
    assert Train.tracked?(cb)
    assert Train.guard(b, @conn) == {:hold, :predecessor_running}
    job = Repo.get_by!(BorsNG.Database.OrchestrationJob, project_id: project.id, kind: "dispatch")
    {id, publisher} = Outbox.payload(job)
    assert id == b.id
    assert publisher.context.staging_ref == Train.ref(cb)
    assert publisher.expected == String.duplicate("0", 40)

    GitHub.ServerMock.put_state(
      put_in(GitHub.ServerMock.get_state(), [@conn, :active_staging_count], 3)
    )

    assert catch_throw(Batcher.resume_dispatch(id, publisher)) == :external_staging_cap
    refute GitHub.ServerMock.get_state()[@conn][:dispatches]

    GitHub.ServerMock.put_state(
      put_in(GitHub.ServerMock.get_state(), [@conn, :active_staging_count], 0)
      |> Map.put(:dispatch_error_after_accept, true)
    )

    assert_raise MatchError, fn -> Batcher.resume_dispatch(id, publisher) end
    Batcher.resume_dispatch(id, publisher)
    assert length(GitHub.ServerMock.get_state()[@conn].dispatches) == 1
    assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
  end

  for {workflow_status, conclusion} <- [
        {nil, nil},
        {"queued", nil},
        {"in_progress", nil},
        {"completed", "failure"},
        {"completed", "success"}
      ] do
    @workflow_status workflow_status
    @conclusion conclusion
    test "exact green statuses reach normal promotion with workflow #{inspect(workflow_status)}/#{inspect(conclusion)}",
         %{project: project} do
      {a, ca, _} = candidate(project, 1)
      Train.reserve(ca, @conn)
      if @workflow_status, do: run(ca, @workflow_status, @conclusion)

      GitHub.ServerMock.put_state(
        put_in(GitHub.ServerMock.get_state(), [@conn, :statuses, a.commit], %{"build" => :ok})
      )

      {:ok, job} = Outbox.claim(Outbox.enqueue(project.id, "info", {:poll, :once}).id)
      Process.put(:orchestration_job, job)
      Batcher.do_handle_cast({:status, {a.commit, "build", :ok, "url"}}, project.id)
      assert Train.event?(ca, "promotion_requested")
      # No Git publisher is configured in this fixture: the normal promotion
      # checks must still prevent claiming that a failed push landed.
      assert Repo.get!(Batch, a.id).state == :running
      assert Repo.get!(Batch, a.id).promotion_failures == 1
      refute Train.event?(ca, "promoted")
      assert GitHub.ServerMock.get_state()[@conn].branches["main"] == @base
      Outbox.finish(job)
      Process.delete(:orchestration_job)
    end
  end

  test "green statuses still honor pending withdrawal without a workflow gate", %{
    project: project
  } do
    {a, ca, [link]} = candidate(project, 1)
    Train.reserve(ca, @conn)

    GitHub.ServerMock.put_state(
      put_in(GitHub.ServerMock.get_state(), [@conn, :statuses, a.commit], %{"build" => :ok})
    )

    {:ok, job} = Outbox.claim(Outbox.enqueue(project.id, "info", {:poll, :once}).id)
    Process.put(:orchestration_job, job)
    Outbox.enqueue(project.id, "cast", {:cancel, link.patch_id, :requested})
    Batcher.do_handle_cast({:status, {a.commit, "build", :ok, "url"}}, project.id)
    refute Train.event?(ca, "promotion_requested")
    assert GitHub.ServerMock.get_state()[@conn].branches["main"] == @base
    Outbox.finish(job)
    Process.delete(:orchestration_job)
  end

  test "lifetime canary budget survives new parent generations", %{project: project} do
    {_, ca, _} = candidate(project, 1)
    assert Train.budget_available?(project.id)
    CandidateJournal.event(ca, "successor_reserved", "test")
    refute Train.budget_available?(project.id)
  end

  test "same-head stale approval cannot resurrect a withdrawn generation", %{project: project} do
    {a, _, [link]} = candidate(project, 1)
    patch = Repo.get!(Patch, link.patch_id)
    old = patch.orchestration_approval_token

    patch
    |> Patch.changeset(%{orchestration_approval_token: "withdrawal-intent"})
    |> Repo.update!()

    QueueApproval.stop([patch], :requested)
    a |> Batch.changeset(%{state: :canceled}) |> Repo.update!()

    Batcher.do_handle_cast(
      {:reviewed_generation, patch.id, patch.commit, old, "old-reviewer"},
      project.id
    )

    refute Repo.get!(Patch, patch.id).queue_pending
    assert Repo.all(Batch.all_for_project(project.id, :incomplete)) == []
  end

  test "immutable barrier evidence rejects a mutable clean file list", %{project: project} do
    {_, ca, _} = candidate(project, 1)
    binding = CandidateJournal.commit_binding(ca)
    assert Train.barrier_free?(ca, @conn)

    for evidence <- [
          %{complete: true, paths: ["scripts/new-audit.py"]},
          %{complete: false, paths: []},
          %{complete: true, paths: List.duplicate("TauCeti/A.lean", 300)}
        ] do
      CandidateJournal.event(ca, "train_barrier_checked", "bors.local_git", %{
        commit: binding["commit"],
        tree: binding["tree"],
        base_sha: @base,
        evidence: evidence
      })

      refute Train.barrier_free?(ca, @conn)
    end

    CandidateJournal.event(ca, "train_barrier_checked", "bors.local_git", %{
      commit: "other",
      tree: binding["tree"],
      base_sha: @base,
      evidence: %{complete: true, paths: []}
    })

    refute Train.barrier_free?(ca, @conn)
  end

  test "incomplete retry transaction cannot discard an innocent approval", %{project: project} do
    {a, ca, _} = candidate(project, 1)
    {b, _, links} = candidate(project, 2, a.commit, ca, 2)
    bundle = Repo.insert!(%BorsNG.Database.PatchBundle{project_id: project.id})

    for link <- links,
        do:
          Repo.get!(Patch, link.patch_id)
          |> Ecto.Changeset.change(bundle_id: bundle.id)
          |> Repo.update!()

    other = Repo.insert!(Batch.new(project.id, "main"))
    first = hd(links)

    CandidateJournal.insert_member(
      LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
        batch_id: other.id,
        patch_id: first.patch_id,
        head_sha: first.head_sha,
        reviewer: first.reviewer
      })
    )

    assert_raise MatchError, fn -> Train.invalidate(b, :predecessor_invalid) end
    assert Repo.get!(Batch, b.id).state == :running
    assert length(Repo.all(LinkPatchBatch.from_batch(b.id))) == 2
    assert Enum.all?(links, &Repo.get!(Patch, &1.patch_id).queue_pending)
    Train.reserve(ca, @conn)
    assert Train.guard(%{a | base_sha: "changed"}, @conn) == {:invalidate, :base_mismatch}
  end

  test "durable intake preserves the latest r+ across prioritized r- and cancel-all", %{
    project: project
  } do
    {a, _, [link]} = candidate(project, 1)
    a |> Batch.changeset(%{state: :canceled}) |> Repo.update!()

    project =
      project |> Project.changeset(%{name: "async-test", batch_delay_sec: 3600}) |> Repo.update!()

    GitHub.ServerMock.put_state(
      GitHub.ServerMock.get_state()
      |> put_in([@conn, :files, "main"], %{"bors.toml" => @toml})
      |> put_in([@conn, :files, "staging.tmp"], %{"bors.toml" => @toml})
    )

    state = %{async_project_id: project.id}

    for command <- [
          {:reviewed, link.patch_id, "old"},
          {:cancel, link.patch_id, :requested},
          {:cancel_all},
          {:reviewed, link.patch_id, "new"}
        ] do
      assert {:reply, :ok, ^state} = Batcher.handle_call({:persist, command}, nil, state)
    end

    jobs =
      Repo.all(
        from(j in BorsNG.Database.OrchestrationJob,
          where: j.project_id == ^project.id,
          order_by: j.id
        )
      )

    [old, cancel, all, latest] = Enum.map(jobs, &Outbox.payload/1)
    assert {:reviewed_generation, _, _, old_token, _} = old
    assert {:reviewed_generation, _, _, latest_token, _} = latest
    refute old_token == latest_token
    assert Repo.get!(Patch, link.patch_id).orchestration_approval_token == latest_token
    # Execute the priority order, with both old withdrawals before approvals.
    Process.put(:orchestration_job, List.last(jobs))
    for command <- [cancel, all, old, latest], do: Batcher.do_handle_cast(command, project.id)
    patch = Repo.get!(Patch, link.patch_id)
    assert patch.queue_pending
    assert patch.queue_approval["reviewer"] == "new"
    [active] = Repo.all(Batch.all_for_patch(link.patch_id, :incomplete))
    assert Repo.one!(LinkPatchBatch.from_batch(active.id)).reviewer == "new"
  end

  test "stale cancellation cannot revoke a reapproved same head", %{project: project} do
    {_a, _ca, [link]} = candidate(project, 1)
    patch = Repo.get!(Patch, link.patch_id)
    old = patch.orchestration_approval_token
    patch |> Patch.changeset(%{orchestration_approval_token: "new-intent"}) |> Repo.update!()
    QueueApproval.remember(Repo.get!(Patch, patch.id), "new-reviewer")

    Batcher.do_handle_cast(
      {:cancel_generation, patch.id, patch.commit, old, :requested},
      project.id
    )

    assert Repo.get!(Patch, patch.id).queue_pending
    assert Repo.one!(Batch.all_for_patch(patch.id, :incomplete)).state == :running
  end
end
