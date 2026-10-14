defmodule BorsNG.Worker.OrchestrationTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    Candidate,
    Installation,
    OrchestrationJob,
    Patch,
    Project,
    Repo,
    Status
  }

  alias BorsNG.Worker.{Batcher, CandidateJournal}
  alias BorsNG.Worker.Orchestration.{Outbox, Worker}
  alias BorsNG.GitHub
  alias BorsNG.GitHub.Pr

  setup do
    # Exercise real executor processes with a deterministic dispatcher. The
    # application dispatcher remains supervised and is resumed after each case.
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "async-test",
        installation_id: inst.id,
        repo_xref: 14,
        staging_branch: "staging"
      })

    conn = {{:installation, 91}, 14}

    GitHub.ServerMock.put_state(%{
      conn => %{
        branches: %{"master" => "ini", "staging" => "old", "staging.tmp" => ""},
        commits: %{},
        comments: %{1 => []},
        statuses: %{"iniN" => %{}},
        files: %{"staging.tmp" => %{"bors.toml" => ~s/status = ["ci", "cd"]/}},
        pulls: %{
          1 => %Pr{
            number: 1,
            title: "Test",
            body: "Body",
            state: :open,
            base_ref: "master",
            head_sha: "N",
            head_ref: "update",
            base_repo_id: 14,
            head_repo_id: 14,
            merged: false
          }
        },
        pr_commits: %{1 => [%GitHub.Commit{sha: "N", author_name: "a", author_email: "e"}]}
      }
    })

    patch =
      Repo.insert!(%Patch{project_id: project.id, pr_xref: 1, commit: "N", into_branch: "master"})

    Batcher.handle_cast({:reviewed, patch.id, "reviewer"}, project.id)
    batch = Repo.get_by!(Batch, project_id: project.id)
    batch |> Batch.changeset(%{last_polled: 0}) |> Repo.update!()
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    worker = start_supervised!({Worker, project.id})

    on_exit(fn ->
      System.delete_env("BORS_ASYNC_ORCHESTRATION")
      System.delete_env("BORS_STAGE_DISPATCH_PROJECT")
      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end)

    {:ok, project: project, patch: patch, batch: batch, conn: conn, worker: worker}
  end

  defp execute(worker, job) do
    Repo.get!(OrchestrationJob, job.id)
    |> Ecto.Changeset.change(available_at: 0, state: "pending")
    |> Repo.update!()

    {:ok, claimed} = Outbox.claim(job.id)
    assert claimed
    send(worker, {:execute, self(), claimed})
    assert_receive {:finished, ^worker, ^claimed, result}, 5000
    if result == :ok, do: Outbox.finish(claimed), else: Outbox.fail(claimed, result)
    result
  end

  defp await_github_call(worker, attempts \\ 500)
  defp await_github_call(_worker, 0), do: flunk("executor did not reach blocked GitHub request")

  defp await_github_call(worker, attempts) do
    {:messages, messages} = Process.info(Process.whereis(GitHub), :messages)

    waiting =
      Enum.any?(messages, fn
        {:"$gen_call", {^worker, _}, _} -> true
        _ -> false
      end)

    unless waiting do
      Process.sleep(10)
      await_github_call(worker, attempts - 1)
    end
  end

  defp construct(ctx) do
    assert :ok == execute(ctx.worker, Outbox.enqueue(ctx.project.id, "info", {:poll, :once}))
    Repo.get_by!(OrchestrationJob, kind: "dispatch", project_id: ctx.project.id)
  end

  test "construction commits binding and dispatch intent before staging mutation", ctx do
    dispatch = construct(ctx)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"]) == "old"
    candidate = CandidateJournal.current(batch)
    assert CandidateJournal.commit_binding(candidate)["commit"] == batch.commit
    assert Outbox.pending?(ctx.project.id, "dispatch:#{batch.id}")
    assert :ok == execute(ctx.worker, dispatch)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"]) == batch.commit
    assert :ok == execute(ctx.worker, dispatch)
    assert Repo.aggregate(from(c in Candidate, where: c.batch_id == ^batch.id), :count) == 1

    assert length(
             Enum.filter(
               CandidateJournal.diagnostics(ctx.project.id, "master", batch.id)
               |> hd()
               |> Map.fetch!(:events),
               &(&1.kind == "dispatched")
             )
           ) == 1
  end

  test "lost dispatch response reconciles accepted workflow without another dispatch", ctx do
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", ctx.project.name)
    job = construct(ctx)

    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :dispatch_error_after_accept, true)
    )

    assert {:error, MatchError} == execute(ctx.worker, job)
    assert length(get_in(GitHub.ServerMock.get_state(), [ctx.conn, :dispatches])) == 1
    assert :ok == execute(ctx.worker, Repo.get!(OrchestrationJob, job.id))
    assert length(get_in(GitHub.ServerMock.get_state(), [ctx.conn, :dispatches])) == 1
    assert Repo.get!(Batch, ctx.batch.id).state == :running
  end

  test "worker death after accepted dispatch recovers across executor restart", ctx do
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", ctx.project.name)
    job = construct(ctx)

    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :dispatch_after_accept_hook, self())
    )

    {:ok, claimed} = Outbox.claim(job.id)
    ref = Process.monitor(ctx.worker)
    send(ctx.worker, {:execute, self(), claimed})
    assert_receive {:dispatch_accepted, github}, 5000
    Process.exit(ctx.worker, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}, 1000
    send(github, :release_dispatch)

    GitHub.ServerMock.put_state(
      Map.delete(GitHub.ServerMock.get_state(), :dispatch_after_accept_hook)
    )

    claimed |> Ecto.Changeset.change(lease_until: 0) |> Repo.update!()
    Outbox.recover_expired()
    {:ok, restarted} = Worker.get(ctx.project.id)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)
    assert :ok == execute(restarted, Repo.get!(OrchestrationJob, job.id))
    assert length(get_in(GitHub.ServerMock.get_state(), [ctx.conn, :dispatches])) == 1
    assert CandidateJournal.latest(ctx.batch.id).generation == 1
  end

  test "worker death before mutation replays durable construction intent", ctx do
    :sys.suspend(GitHub)
    on_exit(fn -> :sys.resume(GitHub) end)
    job = Outbox.enqueue(ctx.project.id, "info", {:poll, :once})
    {:ok, claimed} = Outbox.claim(job.id)
    ref = Process.monitor(ctx.worker)
    send(ctx.worker, {:execute, self(), claimed})
    # Kill only once the executor has returned its sandbox connection and is
    # blocked at the first external read. Killing mid-query destroys MySQL
    # Sandbox ownership, which is a test-fixture failure rather than recovery.
    await_github_call(ctx.worker)
    Process.exit(ctx.worker, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}, 1000
    :sys.resume(GitHub)
    claimed |> Ecto.Changeset.change(lease_until: 0) |> Repo.update!()
    Outbox.recover_expired()
    {:ok, restarted} = Worker.get(ctx.project.id)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)
    assert :ok == execute(restarted, Repo.get!(OrchestrationJob, job.id))
    assert Repo.get!(Batch, ctx.batch.id).state == :running
    assert Outbox.pending?(ctx.project.id, "dispatch:#{ctx.batch.id}")
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"]) == "old"
  end

  test "notification replay suppresses an already visible marker and retains its tombstone",
       ctx do
    marker = "<!-- bors-outbox test-notice -->"

    job =
      Outbox.enqueue(
        ctx.project.id,
        "notice",
        {ctx.conn, 1, "visible " <> marker, marker},
        "notice-test"
      )

    assert :ok == execute(ctx.worker, job)
    assert :ok == execute(ctx.worker, job)

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :comments, 1]) == [
             "visible " <> marker
           ]

    Outbox.enqueue(
      ctx.project.id,
      "notice",
      {ctx.conn, 1, "visible " <> marker, marker},
      "notice-test"
    )

    assert Repo.aggregate(
             from(j in OrchestrationJob, where: j.dedup_key == "notice-test"),
             :count
           ) == 1
  end

  test "rate limit before mutation backs off and lookup failure never dispatches", ctx do
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", ctx.project.name)
    job = construct(ctx)

    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :dispatch_error_before_accept, true)
    )

    assert {:error, MatchError} == execute(ctx.worker, job)
    failed = Repo.get!(OrchestrationJob, job.id)
    assert failed.available_at >= Outbox.now() + 59
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :dispatches]) == nil

    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :dispatch_lookup_error, true)
    )

    assert {:error, MatchError} == execute(ctx.worker, failed)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :dispatches]) == nil

    GitHub.ServerMock.put_state(
      GitHub.ServerMock.get_state()
      |> Map.delete(:dispatch_lookup_error)
      |> Map.delete(:dispatch_error_before_accept)
    )

    assert :ok == execute(ctx.worker, Repo.get!(OrchestrationJob, job.id))
    assert length(get_in(GitHub.ServerMock.get_state(), [ctx.conn, :dispatches])) == 1
  end

  test "durable withdrawal vetoes staging then cancels without an orphan candidate", ctx do
    dispatch = construct(ctx)
    cancellation = Outbox.enqueue(ctx.project.id, "cast", {:cancel, ctx.patch.id, :requested})
    assert {:error, :throw} == execute(ctx.worker, dispatch)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"]) == "old"
    assert :ok == execute(ctx.worker, cancellation)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    assert :ok == execute(ctx.worker, Repo.get!(OrchestrationJob, dispatch.id))
    refute Repo.get!(Patch, ctx.patch.id).queue_pending
  end

  test "changed head rejects stale dispatch and releases its staging slot", ctx do
    dispatch = construct(ctx)
    ctx.patch |> Patch.changeset(%{commit: "new-head"}) |> Repo.update!()
    assert :ok == execute(ctx.worker, dispatch)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"]) == "old"
  end

  test "repository lanes serialize branches while another repository can proceed", ctx do
    first = Outbox.enqueue(ctx.project.id, "info", {:poll, :once})
    second = Outbox.enqueue(ctx.project.id, "info", :restore_rechecks)
    other = Repo.insert!(%Project{name: "other", repo_xref: 99})
    third = Outbox.enqueue(other.id, "info", :restore_rechecks)
    assert {:ok, claimed} = Outbox.claim(first.id)
    assert claimed
    assert {:ok, nil} == Outbox.claim(second.id)
    assert {:ok, other_claimed} = Outbox.claim(third.id)
    assert other_claimed
    Outbox.finish(claimed)
    assert {:ok, next} = Outbox.claim(second.id)
    assert next
  end

  test "a busy repository cannot hide due work behind its backlog", ctx do
    first = Outbox.enqueue(ctx.project.id, "info", :restore_rechecks)
    {:ok, claimed} = Outbox.claim(first.id)
    Enum.each(1..40, fn _ -> Outbox.enqueue(ctx.project.id, "info", :restore_rechecks) end)
    other = Repo.insert!(%Project{name: "other", repo_xref: 99})
    third = Outbox.enqueue(other.id, "info", :restore_rechecks)
    assert Enum.map(Outbox.due(32), & &1.id) == [third.id]
    Outbox.finish(claimed)
  end

  test "deleted repository work terminates without restarting the dispatcher", ctx do
    other = Repo.insert!(%Project{name: "deleted", repo_xref: 98})
    job = Outbox.enqueue(other.id, "info", :restore_rechecks)
    Repo.delete!(other)
    assert {:ok, nil} == Outbox.claim(job.id)
    assert Repo.get!(OrchestrationJob, job.id).state == "done"
    assert Repo.get!(OrchestrationJob, job.id).last_error == "project_deleted"
    assert Repo.get!(Project, ctx.project.id)
  end

  test "durable batch recipes survive the MySQL 64 KiB binary boundary", ctx do
    recipe = %{members: [String.duplicate("immutable attribution ", 5000)]}
    job = Outbox.enqueue(ctx.project.id, "info", recipe)
    assert Outbox.payload(Repo.get!(OrchestrationJob, job.id)) == recipe
  end

  test "restart recovery fences old completion and does not reclaim a live lease", ctx do
    job = Outbox.enqueue(ctx.project.id, "info", :restore_rechecks)
    {:ok, old} = Outbox.claim(job.id)
    Outbox.recover_expired()
    assert Repo.get!(OrchestrationJob, job.id).state == "running"
    old |> Ecto.Changeset.change(lease_until: 0) |> Repo.update!()
    Outbox.recover_expired()
    {:ok, current} = Outbox.claim(job.id)
    assert current.token != old.token
    assert {0, _} = Outbox.finish(old)
    assert {0, _} = Outbox.fail(old, :stale)
    assert Repo.get!(OrchestrationJob, job.id).token == current.token
    Outbox.finish(current)
  end

  test "scheduler accepts work while GitHub is stalled", ctx do
    :sys.suspend(GitHub)
    job = Outbox.enqueue(ctx.project.id, "info", {:poll, :once})
    {:ok, claimed} = Outbox.claim(job.id)
    send(ctx.worker, {:execute, self(), claimed})
    {:ok, scheduler} = Batcher.start_link(ctx.project.id)

    on_exit(fn ->
      if Process.alive?(scheduler), do: GenServer.stop(scheduler)
      :sys.resume(GitHub)
    end)

    assert Batcher.set_priority(scheduler, ctx.patch.id, 7) == :ok

    assert Repo.exists?(
             from(j in OrchestrationJob,
               where: j.project_id == ^ctx.project.id and j.kind == "cast"
             )
           )

    :sys.resume(GitHub)
    assert_receive {:finished, worker, _, :ok}, 5000
    assert worker == ctx.worker
    Outbox.finish(claimed)
  end

  test "duplicate terminal webhook and replay yield one promotion", ctx do
    dispatch = construct(ctx)
    assert :ok == execute(ctx.worker, dispatch)
    batch = Repo.get!(Batch, ctx.batch.id)

    GitHub.ServerMock.put_state(
      put_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, batch.commit], %{
        "ci" => :ok,
        "cd" => :ok
      })
    )

    job = Outbox.enqueue(ctx.project.id, "cast", {:status, {batch.commit, "ci", :ok, "url"}})
    assert :ok == execute(ctx.worker, job)
    assert Repo.get!(Batch, batch.id).state == :ok
    assert :ok == execute(ctx.worker, job)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "master"]) == batch.commit
    events = hd(CandidateJournal.diagnostics(ctx.project.id, "master", batch.id)).events
    assert length(Enum.filter(events, &(&1.kind == "promoted"))) == 1
  end

  test "promotion accepted before crash is recovered without reviving the batch", ctx do
    dispatch = construct(ctx)
    assert :ok == execute(ctx.worker, dispatch)
    batch = Repo.get!(Batch, ctx.batch.id)
    Repo.update_all(Status.all_for_batch(batch.id), set: [state: :ok])

    GitHub.ServerMock.put_state(
      GitHub.ServerMock.get_state()
      |> put_in([ctx.conn, :branches, "master"], batch.commit)
      |> put_in([ctx.conn, :statuses, batch.commit], %{"ci" => :ok, "cd" => :ok})
    )

    batch |> Batch.changeset(%{last_polled: 0}) |> Repo.update!()
    assert :ok == execute(ctx.worker, Outbox.enqueue(ctx.project.id, "info", {:poll, :once}))
    assert Repo.get!(Batch, batch.id).state == :ok

    assert Enum.any?(
             hd(CandidateJournal.diagnostics(ctx.project.id, "master", batch.id)).events,
             &(&1.source == "github.base_recovery_read")
           )
  end

  test "accepted promotion remains successful when main advances before recovery", ctx do
    dispatch = construct(ctx)
    assert :ok == execute(ctx.worker, dispatch)
    batch = Repo.get!(Batch, ctx.batch.id)

    CandidateJournal.event(
      CandidateJournal.current(batch),
      "promotion_requested",
      "bors.outbox",
      %{commit: batch.commit}
    )

    Repo.update_all(Status.all_for_batch(batch.id), set: [state: :ok])

    GitHub.ServerMock.put_state(
      GitHub.ServerMock.get_state()
      |> put_in([ctx.conn, :branches, "master"], "descendant")
      |> put_in([ctx.conn, :compare_status], %{{batch.commit, "descendant"} => :ahead})
      |> put_in([ctx.conn, :statuses, batch.commit], %{"ci" => :ok, "cd" => :ok})
    )

    batch |> Batch.changeset(%{last_polled: 0}) |> Repo.update!()
    assert :ok == execute(ctx.worker, Outbox.enqueue(ctx.project.id, "info", {:poll, :once}))
    assert Repo.get!(Batch, batch.id).state == :ok
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "master"]) == "descendant"

    refute Repo.exists?(
             from(b in Batch, where: b.project_id == ^ctx.project.id and b.state == :waiting)
           )
  end

  test "main advance requeues the tested membership without promoting a stale base", ctx do
    dispatch = construct(ctx)
    assert :ok == execute(ctx.worker, dispatch)
    batch = Repo.get!(Batch, ctx.batch.id)

    GitHub.ServerMock.put_state(
      GitHub.ServerMock.get_state()
      |> put_in([ctx.conn, :push_errors], %{
        "master" => %{error_code: 422, response: "Update is not a fast forward"}
      })
      |> put_in([ctx.conn, :branches, "master"], "advanced")
      |> put_in([ctx.conn, :statuses, batch.commit], %{"ci" => :ok, "cd" => :ok})
    )

    assert :ok ==
             execute(
               ctx.worker,
               Outbox.enqueue(ctx.project.id, "cast", {:status, {batch.commit, "ci", :ok, "url"}})
             )

    assert Repo.get!(Batch, batch.id).state == :error

    assert Repo.exists?(
             from(b in Batch, where: b.project_id == ^ctx.project.id and b.state == :waiting)
           )

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "master"]) == "advanced"
  end
end
