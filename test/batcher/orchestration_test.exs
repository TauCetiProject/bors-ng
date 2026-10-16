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
  alias BorsNG.Worker.Orchestration.{Dispatcher, Outbox, Worker}
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
    :sys.get_state(worker)

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

    case result do
      :ok -> Outbox.finish(claimed)
      {:defer, reason} -> Outbox.defer(claimed, reason)
      error -> Outbox.fail(claimed, error)
    end

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

  defp dispatcher do
    {:ok, pid} = GenServer.start_link(Dispatcher, [])
    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    pid
  end

  defp await_condition(fun, attempts \\ 500)
  defp await_condition(_fun, 0), do: flunk("condition did not become true")

  defp await_condition(fun, attempts) do
    unless fun.() do
      Process.sleep(10)
      await_condition(fun, attempts - 1)
    end
  end

  defp await_shutdown(pid) do
    await_condition(fn ->
      Process.info(pid, :current_function) ==
        {:current_function, {Dispatcher, :await_shutdown, 3}}
    end)
  end

  defp stale_messages(pid, worker, claimed) do
    if Process.alive?(pid) do
      send(pid, {:finished, worker, %{claimed | token: "stale"}, :ok})
      Process.sleep(1)
      stale_messages(pid, worker, claimed)
    end
  end

  test "normal shutdown drains accepted dispatch while a second dispatcher preserves cancellation",
       ctx do
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", ctx.project.name)
    job = construct(ctx)

    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :dispatch_after_accept_hook, self())
    )

    # Reverse supervisor stop order leaves the worker alive during the drain.
    stop_supervised!(Worker)

    {:ok, supervisor} =
      Supervisor.start_link(
        [
          {Worker, ctx.project.id},
          %{id: Dispatcher, start: {GenServer, :start_link, [Dispatcher, []]}}
        ],
        strategy: :one_for_all
      )

    Process.unlink(supervisor)
    on_exit(fn -> if Process.alive?(supervisor), do: Supervisor.stop(supervisor) end)
    {Dispatcher, first, _, _} = List.keyfind(Supervisor.which_children(supervisor), Dispatcher, 0)
    [{worker, _}] = Registry.lookup(BorsNG.Worker.Orchestration.Registry, ctx.project.id)
    send(first, :tick)
    assert_receive {:dispatch_accepted, github}, 5000
    claimed = Repo.get!(OrchestrationJob, job.id)
    cancel = Outbox.enqueue(ctx.project.id, "cast", {:cancel, ctx.patch.id})
    second = dispatcher()
    send(second, :tick)
    assert :sys.get_state(second).active == %{}
    assert Repo.get!(OrchestrationJob, cancel.id).state == "pending"

    started = System.monotonic_time(:millisecond)
    stopping = Task.async(fn -> Supervisor.stop(supervisor, :shutdown, 5000) end)
    await_shutdown(first)
    # A stale result cannot free the lane during shutdown.
    send(first, {:finished, worker, %{claimed | token: "stale"}, :ok})
    assert Repo.get!(OrchestrationJob, job.id).state == "running"
    assert {:ok, nil} == Outbox.claim(cancel.id)
    send(github, :release_dispatch)
    assert Task.await(stopping, 5000) == :ok
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed < 4000
    refute Process.alive?(worker)
    assert Repo.get!(OrchestrationJob, job.id).state == "done"
    assert Repo.get!(OrchestrationJob, job.id).attempts == 1

    assert Enum.any?(
             Repo.get!(OrchestrationJob, job.id).attempt_timing["recent"],
             &(&1["event"] == "shutdown_result")
           )

    assert claimed.lease_until > Outbox.now() + 250

    # Restart/another process can now claim without waiting for lease expiry.
    # A completion delivered to the wrong dispatcher still cannot change it.
    send(second, {:finished, ctx.worker, claimed, :ok})
    send(second, :tick)
    await_condition(fn -> Repo.get!(OrchestrationJob, cancel.id).state == "done" end)
    assert length(get_in(GitHub.ServerMock.get_state(), [ctx.conn, :dispatches])) == 1
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    assert {0, _} = Outbox.finish(claimed)
    assert {0, _} = Outbox.renew(claimed)
  end

  test "shutdown deadline retains unfinished lease and fences late completion after recovery",
       ctx do
    :sys.suspend(GitHub)
    on_exit(fn -> :sys.resume(GitHub) end)
    job = Outbox.enqueue(ctx.project.id, "info", {:poll, :once})
    first = dispatcher()
    send(first, :tick)
    await_github_call(ctx.worker)
    claimed = Repo.get!(OrchestrationJob, job.id)
    next = Outbox.enqueue(ctx.project.id, "cast", {:cancel, ctx.patch.id})

    # Continuing stale completions cannot extend the absolute deadline.
    spawn(fn -> stale_messages(first, ctx.worker, claimed) end)
    started = System.monotonic_time(:millisecond)
    assert :ok == GenServer.stop(first, :shutdown, 5000)
    assert System.monotonic_time(:millisecond) - started < 5000
    assert Repo.get!(OrchestrationJob, job.id).token == claimed.token
    second = dispatcher()
    send(second, :tick)
    assert :sys.get_state(second).active == %{}
    assert {:ok, nil} == Outbox.claim(next.id)

    # Model the supervisor stopping the old executor after the drain deadline.
    # Only expiry can recover it; a late old result cannot complete a new claim.
    ref = Process.monitor(ctx.worker)
    Process.exit(ctx.worker, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}, 1000
    :sys.resume(GitHub)
    claimed |> Ecto.Changeset.change(lease_until: 0) |> Repo.update!()
    assert {1, _} = Outbox.recover_expired()
    {:ok, current} = Outbox.claim(job.id)
    assert current.token != claimed.token
    send(second, {:finished, ctx.worker, claimed, :ok})
    :sys.get_state(second)
    assert {0, _} = Outbox.finish(claimed)
    assert {0, _} = Outbox.fail(claimed, :late)
    assert {0, _} = Outbox.renew(claimed)
    assert Repo.get!(OrchestrationJob, job.id).token == current.token
    assert nil == Outbox.record_shutdown_result(claimed)

    refute Enum.any?(
             Repo.get!(OrchestrationJob, job.id).attempt_timing["recent"],
             &(&1["event"] == "shutdown_result")
           )

    Outbox.finish(current)
  end

  test "abrupt dispatcher death does not free an accepted external mutation", ctx do
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", ctx.project.name)
    job = construct(ctx)

    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :dispatch_after_accept_hook, self())
    )

    first = dispatcher()
    Process.unlink(first)
    send(first, :tick)
    assert_receive {:dispatch_accepted, github}, 5000
    claimed = Repo.get!(OrchestrationJob, job.id)
    ref = Process.monitor(first)
    Process.exit(first, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}, 1000
    assert {0, _} = Outbox.recover_expired()
    assert {:ok, nil} == Outbox.claim(job.id)
    send(github, :release_dispatch)
    # Ensure the first command returned before simulating lease expiry.
    :sys.get_state(ctx.worker)

    GitHub.ServerMock.put_state(
      Map.delete(GitHub.ServerMock.get_state(), :dispatch_after_accept_hook)
    )

    claimed |> Ecto.Changeset.change(lease_until: 0) |> Repo.update!()
    assert {1, _} = Outbox.recover_expired()
    assert :ok == execute(ctx.worker, Repo.get!(OrchestrationJob, job.id))
    assert length(get_in(GitHub.ServerMock.get_state(), [ctx.conn, :dispatches])) == 1
  end

  test "attempt timing preserves overwritten service and recovery evidence within a fixed bound",
       ctx do
    job = Outbox.enqueue(ctx.project.id, "info", :restore_rechecks)
    {:ok, first} = Outbox.claim(job.id)
    Outbox.fail(first, :retry)
    finished = Repo.get!(OrchestrationJob, job.id)
    finished |> Ecto.Changeset.change(available_at: 0) |> Repo.update!()
    {:ok, second} = Outbox.claim(job.id)
    [event | _] = second.attempt_timing["recent"]
    assert event["previous_claimed_at_ms"] == first.claimed_at_ms
    assert event["previous_completed_at_ms"] == finished.completed_at_ms
    assert second.completed_at_ms == nil

    for _ <- 1..6 do
      Repo.get!(OrchestrationJob, job.id)
      |> Ecto.Changeset.change(lease_until: 0)
      |> Repo.update!()

      assert {1, _} = Outbox.recover_expired()
      assert {:ok, %OrchestrationJob{}} = Outbox.claim(job.id)
    end

    history = Repo.get!(OrchestrationJob, job.id).attempt_timing["recent"]
    assert length(history) == 8
    assert Enum.any?(history, &(&1["event"] == "lease_expired" and is_integer(&1["at_ms"])))
    refute Jason.encode!(history) =~ first.token
  end

  test "one dispatcher turn locks one job per repository despite hundreds of due rows", ctx do
    dispatch = construct(ctx)

    Repo.delete_all(
      from(j in OrchestrationJob, where: j.project_id == ^ctx.project.id and j.id != ^dispatch.id)
    )

    for n <- 1..300 do
      Outbox.enqueue(ctx.project.id, "cast", {:status, {"noise#{n}", "ordinary", :ok, "url"}})
    end

    test_pid = self()
    handler = {__MODULE__, make_ref()}
    prefix = Repo.config()[:telemetry_prefix]

    :ok =
      :telemetry.attach(
        handler,
        prefix ++ [:query],
        fn _event, _measurements, metadata, _config ->
          if self() == test_pid and metadata.source == "projects" and
               String.contains?(metadata.query, "FOR UPDATE"),
             do: send(test_pid, :project_claim_lock)
        end,
        nil
      )

    :sys.suspend(GitHub)
    Process.delete(:last_drain_at)
    Process.delete(:last_renew_at)
    old_cap = System.get_env("BORS_ASYNC_WORKERS")
    System.put_env("BORS_ASYNC_WORKERS", "2")

    try do
      {:noreply, state} =
        BorsNG.Worker.Orchestration.Dispatcher.handle_info(:tick, %{active: %{}})

      assert map_size(state.active) == 1
      {:messages, messages} = Process.info(self(), :messages)
      assert Enum.count(messages, &(&1 == :project_claim_lock)) == 1
      assert Repo.get!(OrchestrationJob, dispatch.id).state == "running"
      notification_worker(ctx)
      Outbox.enqueue(ctx.project.id, "notice", {ctx.conn, 1, "message", "marker"})
      Process.delete(:last_drain_at)
      {:noreply, state} = BorsNG.Worker.Orchestration.Dispatcher.handle_info(:tick, state)
      assert map_size(state.active) == 2
      :sys.resume(GitHub)

      state =
        Enum.reduce(1..2, state, fn _, state ->
          assert_receive {:finished, worker, claimed, :ok}, 5000

          {:noreply, state} =
            BorsNG.Worker.Orchestration.Dispatcher.handle_info(
              {:finished, worker, claimed, :ok},
              state
            )

          state
        end)

      assert state.active == %{}
    after
      :telemetry.detach(handler)
      if Process.get(:tick_timer), do: Process.cancel_timer(Process.get(:tick_timer))
      :sys.resume(GitHub)

      if old_cap,
        do: System.put_env("BORS_ASYNC_WORKERS", old_cap),
        else: System.delete_env("BORS_ASYNC_WORKERS")
    end
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
    assert {:defer, :withdrawal_pending} == execute(ctx.worker, dispatch)
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

  test "dispatch is next after the occupied operation despite 400 irrelevant jobs", ctx do
    dispatch = construct(ctx)

    Repo.update_all(
      from(j in OrchestrationJob,
        where: j.project_id == ^ctx.project.id and j.id != ^dispatch.id and j.state == "pending"
      ),
      set: [state: "done"]
    )

    occupied = Outbox.enqueue(ctx.project.id, "info", :restore_rechecks)
    {:ok, running} = Outbox.claim(occupied.id)

    Enum.each(1..400, fn n ->
      Outbox.enqueue(ctx.project.id, "cast", {:status, {"irrelevant-#{n}", "ci", :ok, "url"}})
    end)

    # Also put the dispatch behind the entire backlog by durable FIFO ID.
    Repo.delete!(dispatch)
    dispatch = Outbox.enqueue(ctx.project.id, "dispatch", Outbox.payload(dispatch))
    # Repository lanes are independent: another project's recovery may be due.
    own_due = fn -> Enum.filter(Outbox.due(32), &(&1.project_id == ctx.project.id)) end
    assert own_due.() == []
    Outbox.finish(running)
    assert hd(own_due.()).id == dispatch.id
    assert :ok == execute(ctx.worker, dispatch)

    assert Repo.get!(Batch, ctx.batch.id).commit ==
             get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"])

    assert Repo.aggregate(
             from(j in OrchestrationJob,
               where: j.project_id == ^ctx.project.id and j.state == "pending" and j.priority == 2
             ),
             :count
           ) >= 400
  end

  test "background FIFO gets one turn after eight critical claims across a worker restart", ctx do
    Repo.update_all(from(j in OrchestrationJob, where: j.project_id == ^ctx.project.id),
      set: [state: "done"]
    )

    background = Outbox.enqueue(ctx.project.id, "info", :recover_queue)
    later = Outbox.enqueue(ctx.project.id, "info", :wake_backend_holds)
    critical = Enum.map(1..10, fn n -> Outbox.enqueue(ctx.project.id, "dispatch", n) end)

    Enum.each(Enum.take(critical, 8), fn expected ->
      assert hd(Outbox.due(32)).id == expected.id
      {:ok, claimed} = Outbox.claim(expected.id)
      Outbox.finish(claimed)
    end)

    # Fairness state is in the locked project row, not the executor's mailbox.
    GenServer.stop(ctx.worker)
    {:ok, restarted} = Worker.get(ctx.project.id)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted) end)
    assert hd(Outbox.due(32)).id == background.id
    {:ok, claimed} = Outbox.claim(background.id)
    Outbox.finish(claimed)
    assert hd(Outbox.due(32)).id == Enum.at(critical, 8).id
    assert later.priority == 2
  end

  test "all active candidate observations retain FIFO ahead of ordinary statuses", ctx do
    dispatch = construct(ctx)
    batch = Repo.get!(Batch, ctx.batch.id)
    ordinary = Outbox.enqueue(ctx.project.id, "cast", {:status, {"pr-head", "ci", :ok, "url"}})

    observations =
      Enum.map([:ok, :error, :waiting], fn state ->
        Outbox.enqueue(ctx.project.id, "cast", {:status, {batch.commit, "ci", state, "url"}})
      end)

    # An earlier green receipt can have deferred while processing a withdrawal.
    # It must retain FIFO relative to the later red, rather than move behind it.
    hd(observations)
    |> Ecto.Changeset.change(last_error: "deferred:withdrawal_pending")
    |> Repo.update!()

    ids = Enum.map(observations, & &1.id)
    due = Enum.map(Outbox.due(32), & &1.id)
    assert Enum.filter(due, &(&1 in ids)) == ids
    assert Enum.all?(observations, &(&1.priority == 1))
    assert ordinary.priority == 2

    assert Enum.find_index(due, &(&1 == List.last(ids))) <
             Enum.find_index(due, &(&1 == ordinary.id))

    assert dispatch.priority == 1
  end

  test "a command received before construction is upgraded once its member is running", ctx do
    command = Outbox.enqueue(ctx.project.id, "cast", {:set_priority, ctx.patch.id, 7})
    assert command.priority == 2
    # Construct while a durable command is still pending. The command must
    # outrank a ready dispatch even though its receipt-time class was background.
    assert {:defer, :withdrawal_pending} ==
             execute(ctx.worker, Outbox.enqueue(ctx.project.id, "info", {:poll, :once}))

    # Construction itself is fenced; simulate the same state transition another
    # scheduler can commit between classification and insertion of the command.
    ctx.batch |> Batch.changeset(%{state: :running}) |> Repo.update!()
    dispatch = Outbox.enqueue(ctx.project.id, "dispatch", ctx.batch.id)
    assert hd(Outbox.due(32)).id == command.id
    {:ok, claimed} = Outbox.claim(command.id)
    assert claimed.priority == 0
    Outbox.finish(claimed)
    assert dispatch.priority == 1
  end

  test "a deferred poll yields to waiting-member commands after its backoff expires", ctx do
    Repo.update_all(from(j in OrchestrationJob, where: j.project_id == ^ctx.project.id),
      set: [state: "done"]
    )

    poll = Outbox.enqueue(ctx.project.id, "info", {:poll, :once})
    command = Outbox.enqueue(ctx.project.id, "cast", {:set_priority, ctx.patch.id, 7})
    assert command.priority == 2
    assert {:defer, :withdrawal_pending} == execute(ctx.worker, poll)
    assert Repo.get!(Batch, ctx.batch.id).state == :waiting

    # Reproduce a normal tick after the one-second defer deadline. The older
    # priority-1 poll and dynamically upgraded waiting-member command now tie.
    # Reclaiming the poll first would repeatedly hit the same withdrawal fence.
    Repo.get!(OrchestrationJob, poll.id)
    |> Ecto.Changeset.change(available_at: Outbox.now() - 1)
    |> Repo.update!()

    own_due = Enum.filter(Outbox.due(32), &(&1.project_id == ctx.project.id))
    assert hd(own_due).id == command.id
    assert :ok == execute(ctx.worker, command)
    assert :ok == execute(ctx.worker, Repo.get!(OrchestrationJob, poll.id))
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running
    assert batch.commit == "iniN"
    assert Outbox.pending?(ctx.project.id, "dispatch:#{batch.id}")
  end

  test "same-PR commands retain FIFO when a batch finishes before either runs", ctx do
    older = Outbox.enqueue(ctx.project.id, "cast", {:set_priority, ctx.patch.id, 10})
    ctx.batch |> Batch.changeset(%{state: :running}) |> Repo.update!()
    newer = Outbox.enqueue(ctx.project.id, "cast", {:set_priority, ctx.patch.id, 5})
    ctx.batch |> Batch.changeset(%{state: :canceled}) |> Repo.update!()

    assert Enum.filter(Enum.map(Outbox.due(32), & &1.id), &(&1 in [older.id, newer.id])) == [
             older.id,
             newer.id
           ]

    assert :ok == execute(ctx.worker, older)
    assert :ok == execute(ctx.worker, newer)
    assert Repo.get!(Patch, ctx.patch.id).priority == 5
  end

  test "an unjournaled legacy running batch can promote in the executor", ctx do
    ctx.batch
    |> Batch.changeset(%{
      state: :running,
      commit: "iniN",
      base_sha: "ini",
      timeout_at: Outbox.now() + 300
    })
    |> Repo.update!()

    for identifier <- ["ci", "cd"],
        do: Repo.insert!(%Status{batch_id: ctx.batch.id, identifier: identifier, state: :waiting})

    GitHub.ServerMock.put_state(
      put_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, "iniN"], %{
        "ci" => :ok,
        "cd" => :ok
      })
    )

    assert CandidateJournal.current(Repo.get!(Batch, ctx.batch.id)) == nil

    assert :ok ==
             execute(
               ctx.worker,
               Outbox.enqueue(ctx.project.id, "cast", {:status, {"iniN", "ci", :ok, "url"}})
             )

    assert Repo.get!(Batch, ctx.batch.id).state == :ok
  end

  test "eligibility received before a patch exists conservatively fences the project", ctx do
    dispatch = construct(ctx)
    policy = Outbox.enqueue(ctx.project.id, "cast", {:eligibility, 999_999})
    assert policy.subject_id == 0 and policy.priority == 0
    assert {:defer, :withdrawal_pending} == execute(ctx.worker, dispatch)
    assert :ok == execute(ctx.worker, policy)
    assert :ok == execute(ctx.worker, Repo.get!(OrchestrationJob, dispatch.id))
  end

  test "pending policy and other candidate commands fence dispatch even in backoff", ctx do
    dispatch = construct(ctx)

    policy =
      Outbox.enqueue(
        ctx.project.id,
        "cast",
        {:eligibility, ctx.patch.pr_xref},
        nil,
        Outbox.now() + 3600
      )

    assert policy.subject_id == ctx.patch.id
    assert {:defer, :withdrawal_pending} == execute(ctx.worker, dispatch)
    deferred = Repo.get!(OrchestrationJob, dispatch.id)
    assert deferred.attempts == 0
    assert deferred.available_at >= policy.available_at
    assert deferred.last_error == "deferred:withdrawal_pending"
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"]) == "old"
    assert :ok == execute(ctx.worker, policy)
    assert Repo.get!(OrchestrationJob, dispatch.id).available_at <= Outbox.now()
    command = Outbox.enqueue(ctx.project.id, "cast", {:set_priority, ctx.patch.id, 7})

    assert {:defer, :withdrawal_pending} ==
             execute(ctx.worker, Repo.get!(OrchestrationJob, dispatch.id))

    assert :ok == execute(ctx.worker, command)
    assert :ok == execute(ctx.worker, Repo.get!(OrchestrationJob, dispatch.id))
  end

  test "live head change without a processed webhook blocks prioritized dispatch", ctx do
    dispatch = construct(ctx)

    GitHub.ServerMock.put_state(
      put_in(
        GitHub.ServerMock.get_state(),
        [ctx.conn, :pulls, 1, Access.key(:head_sha)],
        "new-live-head"
      )
    )

    assert {:error, {:throw, :stale_candidate_member}} == execute(ctx.worker, dispatch)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"]) == "old"
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
  end

  test "stale green never promotes over current red", ctx do
    dispatch = construct(ctx)
    assert :ok == execute(ctx.worker, dispatch)
    batch = Repo.get!(Batch, ctx.batch.id)

    GitHub.ServerMock.put_state(
      put_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, batch.commit], %{
        "ci" => :error,
        "cd" => :ok
      })
    )

    green =
      Outbox.enqueue(ctx.project.id, "cast", {:status, {batch.commit, "ci", :ok, "old-green"}})

    assert :ok == execute(ctx.worker, green)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "master"]) == "ini"
    refute Repo.get!(Batch, batch.id).state == :ok
  end

  test "operation telemetry separates ready wait and service, including failed attempts", ctx do
    handler = "outbox-test-#{ctx.project.id}"

    :telemetry.attach(
      handler,
      [:bors, :outbox, :operation],
      fn _, measures, metadata, pid ->
        send(pid, {:operation_measured, measures, metadata})
      end,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    job = Outbox.enqueue(ctx.project.id, "info", :restore_rechecks)
    assert :ok == execute(ctx.worker, job)

    assert_receive {:operation_measured, %{ready_wait_ms: wait, service_ms: service},
                    %{job_id: id, result: :ok}}

    assert id == job.id and wait >= 0 and service >= 0
    done = Repo.get!(OrchestrationJob, job.id)
    assert done.completed_at_ms >= done.claimed_at_ms
  end

  defp notification_worker(ctx) do
    {:ok, worker} = Worker.get({:notification, ctx.project.id})
    on_exit(fn -> if Process.alive?(worker), do: GenServer.stop(worker) end)
    :sys.get_state(worker)
    worker
  end

  test "notification leases cannot block a mutator or permit a second mutator", ctx do
    notice = Outbox.enqueue(ctx.project.id, "notice", {ctx.conn, 1, "first", "first"})
    later_notice = Outbox.enqueue(ctx.project.id, "notice", {ctx.conn, 1, "second", "second"})
    mutation = Outbox.enqueue(ctx.project.id, "info", :restore_rechecks)
    other_mutation = Outbox.enqueue(ctx.project.id, "info", :restore_rechecks)
    {:ok, n} = Outbox.claim(notice.id)
    assert n
    assert Enum.any?(Outbox.due(32), &(&1.id == mutation.id))
    {:ok, m} = Outbox.claim(mutation.id)
    assert m
    assert {:ok, nil} == Outbox.claim(other_mutation.id)
    assert {:ok, nil} == Outbox.claim(later_notice.id)
    Outbox.finish(m)
    assert {:ok, next} = Outbox.claim(other_mutation.id)
    assert next
    Outbox.finish(next)
    Outbox.finish(n)
    assert {:ok, later} = Outbox.claim(later_notice.id)
    assert later
  end

  test "notification executor rejects mutation work and ignores legacy poll messages", ctx do
    worker = notification_worker(ctx)
    count = Repo.aggregate(OrchestrationJob, :count)
    send(worker, :recover_queue)
    :sys.get_state(worker)
    assert Repo.aggregate(OrchestrationJob, :count) == count
    mutation = Outbox.enqueue(ctx.project.id, "dispatch", ctx.batch.id)
    assert {:error, {:throw, :wrong_notification_lane}} == execute(worker, mutation)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "staging"]) == "old"
  end

  test "notification FIFO includes retry backoff and late green cannot overwrite canceled red",
       ctx do
    worker = notification_worker(ctx)
    ctx.batch |> Batch.changeset(%{state: :running, commit: "iniN"}) |> Repo.update!()
    green_payload = {:status, ctx.conn, {"N", :ok, "green", "url"}, ctx.batch.id}
    red_payload = {:status, ctx.conn, {"N", :error, "red", "url"}, ctx.batch.id}
    green = Outbox.enqueue(ctx.project.id, "projection", green_payload, "green-projection")
    red = Outbox.enqueue(ctx.project.id, "projection", red_payload, "red-projection")
    {:ok, claimed} = Outbox.claim(green.id)
    Outbox.fail(claimed, :unknown_response)
    assert Repo.get!(OrchestrationJob, green.id).available_at >= Outbox.now() + 299
    assert Outbox.due(32, :notification) == []
    assert {:ok, nil} == Outbox.claim(red.id)
    ctx.batch |> Batch.changeset(%{state: :canceled}) |> Repo.update!()
    assert :ok == execute(worker, green)
    assert :ok == execute(worker, red)
    late = Outbox.enqueue(ctx.project.id, "projection", green_payload, "late-green")
    assert :ok == execute(worker, late)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, "N", "bors"]) == :error
    Outbox.enqueue(ctx.project.id, "projection", red_payload, "red-projection")

    assert Repo.aggregate(
             from(j in OrchestrationJob, where: j.dedup_key == "red-projection"),
             :count
           ) == 1
  end

  test "canceling a lone waiting PR delivers red after deleting its batch", ctx do
    worker = notification_worker(ctx)

    assert :ok ==
             execute(
               ctx.worker,
               Outbox.enqueue(ctx.project.id, "cast", {:cancel, ctx.patch.id, :requested})
             )

    refute Repo.get(Batch, ctx.batch.id)

    [projection] =
      Repo.all(
        from(j in OrchestrationJob,
          where: j.project_id == ^ctx.project.id and j.kind == "projection"
        )
      )

    assert :ok == execute(worker, projection)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, "N", "bors"]) == :error
  end

  test "notification failures terminate visibly after six attempts and release FIFO", ctx do
    first = Outbox.enqueue(ctx.project.id, "projection", :unused, "exhausted-projection")
    next = Outbox.enqueue(ctx.project.id, "notice", :unused, "following-notice")

    Enum.each(1..6, fn _ ->
      Repo.get!(OrchestrationJob, first.id)
      |> Ecto.Changeset.change(available_at: 0)
      |> Repo.update!()

      {:ok, owned} = Outbox.claim(first.id)
      assert {:ok, nil} == Outbox.claim(next.id)
      Outbox.fail(owned, :infrastructure)
    end)

    retired = Repo.get!(OrchestrationJob, first.id)
    assert retired.state == "done"
    assert retired.dedup_key == "exhausted-projection"
    assert retired.last_error =~ "notification_terminal:"
    assert {:ok, owned} = Outbox.claim(next.id)
    assert owned
    Outbox.fail(owned, {:terminal, {:http_status, 404}})
    assert Repo.get!(OrchestrationJob, next.id).state == "done"
  end

  test "a queued advisory never overwrites a SHA later bound as a tested commit", ctx do
    worker = notification_worker(ctx)

    old =
      Outbox.enqueue(
        ctx.project.id,
        "projection",
        {:status, ctx.conn, {"N", :running, "old pending", "url"}, ctx.batch.id}
      )

    ctx.batch |> Batch.changeset(%{state: :running, commit: "N"}) |> Repo.update!()
    assert :ok == GitHub.post_commit_status(ctx.conn, {"N", :ok, "tested green", "url"})
    assert :ok == execute(worker, old)
    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, "N", "bors"]) == :ok
  end

  for recovery <- [false, true] do
    test "squash promotion durably defers PR closing (recovery=#{recovery})", ctx do
      mock = GitHub.ServerMock.get_state()

      mock =
        put_in(
          mock,
          [ctx.conn, :files, "staging.tmp", "bors.toml"],
          "status = [\"ci\", \"cd\"]\nuse_squash_merge = true"
        )

      mock =
        put_in(mock, [ctx.conn, :pulls, 1, Access.key(:user)], %GitHub.User{
          id: 42,
          login: "ada",
          avatar_url: "url"
        })

      GitHub.ServerMock.put_state(mock)
      dispatch = construct(ctx)
      assert :ok == execute(ctx.worker, dispatch)
      batch = Repo.get!(Batch, ctx.batch.id)

      mock =
        put_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, batch.commit], %{
          "ci" => :ok,
          "cd" => :ok
        })

      mock =
        if unquote(recovery),
          do: put_in(mock, [ctx.conn, :branches, "master"], batch.commit),
          else: mock

      GitHub.ServerMock.put_state(mock)

      assert :ok ==
               execute(
                 ctx.worker,
                 Outbox.enqueue(
                   ctx.project.id,
                   "cast",
                   {:status, {batch.commit, "ci", :ok, "url"}}
                 )
               )

      assert Repo.get!(Batch, batch.id).state == :ok

      assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "master"]) ==
               batch.commit

      assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 1, Access.key(:state)]) ==
               :open

      [source] =
        Repo.all(
          from(j in OrchestrationJob,
            where: j.project_id == ^ctx.project.id and j.kind == "info"
          )
        )
        |> Enum.filter(&match?({:post_merge_cleanup, _, _, _, _, _}, Outbox.payload(&1)))

      assert :ok == execute(ctx.worker, source)
      [child] = cleanup_children(ctx)
      assert :ok == execute(ctx.worker, child)

      assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 1, Access.key(:state)]) ==
               :closed
    end
  end

  defp completed_cleanup(ctx, count, squash \\ true) do
    ctx.batch |> Batch.changeset(%{state: :ok, commit: "completed"}) |> Repo.update!()

    patches = [
      ctx.patch
      | for(
          n <- 2..max(count, 2),
          count > 1,
          do:
            Repo.insert!(%Patch{
              project_id: ctx.project.id,
              pr_xref: n,
              commit: "N#{n}",
              into_branch: "master"
            })
        )
    ]

    mock = GitHub.ServerMock.get_state()
    pulls = get_in(mock, [ctx.conn, :pulls])

    pulls =
      Enum.reduce(patches, pulls, fn p, acc ->
        Map.put(acc, p.pr_xref, %Pr{
          number: p.pr_xref,
          title: "Title",
          body: "Keep body",
          state: :open,
          head_sha: p.commit,
          base_ref: "master",
          draft: false
        })
      end)

    GitHub.ServerMock.put_state(put_in(mock, [ctx.conn, :pulls], pulls))

    members =
      Enum.map(patches, fn p ->
        %{
          patch_id: p.id,
          pr: p.pr_xref,
          head: p.commit,
          token: p.orchestration_approval_token,
          body: "Merged exact head #{p.commit}"
        }
      end)

    source =
      Outbox.enqueue(
        ctx.project.id,
        "info",
        {:post_merge_cleanup, ctx.batch.id, "completed", nil, squash, members},
        "post-merge:#{ctx.batch.id}:completed"
      )

    assert :ok == execute(ctx.worker, source)
    {source, patches}
  end

  defp cleanup_children(ctx) do
    Repo.all(
      from(j in OrchestrationJob,
        where: j.project_id == ^ctx.project.id and j.kind == "info",
        order_by: j.id
      )
    )
    |> Enum.filter(&match?({:close_merged_pr, _, _, _, _}, Outbox.payload(&1)))
  end

  test "post-merge bookkeeping expands four rows per turn and closes one exact PR per job", ctx do
    {source, patches} = completed_cleanup(ctx, 9)
    assert length(cleanup_children(ctx)) == 4

    for offset <- [4, 8] do
      [continuation] =
        Repo.all(
          from(j in OrchestrationJob,
            where: j.project_id == ^ctx.project.id and j.state == "pending"
          )
        )
        |> Enum.filter(&(Outbox.payload(&1) == {:expand_post_merge_cleanup, source.id, offset}))

      assert :ok == execute(ctx.worker, continuation)
    end

    children = cleanup_children(ctx)
    assert length(children) == 9
    [first | rest] = children
    assert :ok == execute(ctx.worker, first)
    pr = get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, hd(patches).pr_xref])
    assert pr.state == :closed
    assert pr.title == "[Merged by Bors] - Title"
    assert pr.body == "Keep body"

    Enum.each(tl(patches), fn p ->
      assert get_in(GitHub.ServerMock.get_state(), [
               ctx.conn,
               :pulls,
               p.pr_xref,
               Access.key(:state)
             ]) == :open
    end)

    assert :ok == execute(ctx.worker, first)

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 1, Access.key(:title)]) ==
             "[Merged by Bors] - Title"

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :branches, "master"]) == "ini"
    assert length(rest) == 8
    # Completed children retain keys: replay cannot regenerate already done work.
    duplicate =
      Outbox.enqueue(
        ctx.project.id,
        "info",
        {:expand_post_merge_cleanup, source.id, 0},
        "duplicate-expansion"
      )

    assert :ok == execute(ctx.worker, duplicate)
    assert length(cleanup_children(ctx)) == 9
  end

  test "cleanup cannot close a changed head or a new approval generation", ctx do
    {_source, patches} = completed_cleanup(ctx, 2)
    [one, two] = cleanup_children(ctx)
    [p1, p2] = patches
    p1 |> Patch.changeset(%{commit: "new-head"}) |> Repo.update!()

    p2
    |> Ecto.Changeset.change(orchestration_approval_token: "fresh-generation")
    |> Repo.update!()

    assert :ok == execute(ctx.worker, one)
    assert :ok == execute(ctx.worker, two)

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 1, Access.key(:state)]) ==
             :open

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 2, Access.key(:state)]) ==
             :open

    assert Repo.aggregate(
             from(j in OrchestrationJob,
               where: j.project_id == ^ctx.project.id and j.kind == "notice"
             ),
             :count
           ) == 2
  end

  test "cleanup respects pending candidate commands and retries one PR without blocking its siblings",
       ctx do
    {_source, _patches} = completed_cleanup(ctx, 2)
    [one, two] = cleanup_children(ctx)
    reconsider = Outbox.enqueue(ctx.project.id, "cast", {:eligibility, 1})
    assert {:defer, :withdrawal_pending} == execute(ctx.worker, one)
    assert :ok == execute(ctx.worker, reconsider)
    GitHub.ServerMock.put_state(Map.put(GitHub.ServerMock.get_state(), :get_pr_error, 1))
    assert {:error, {:throw, :post_merge_read_unavailable}} == execute(ctx.worker, one)
    assert Repo.get!(OrchestrationJob, one.id).available_at > Outbox.now()
    assert :ok == execute(ctx.worker, two)

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 2, Access.key(:state)]) ==
             :closed

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 1, Access.key(:state)]) ==
             :open

    assert :ok == execute(ctx.worker, one)

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 1, Access.key(:state)]) ==
             :closed
  end

  test "cleanup uses its own client while notifications are occupied", ctx do
    {_source, _patches} = completed_cleanup(ctx, 1)
    [child] = cleanup_children(ctx)
    :sys.suspend(GitHub.Advisory)

    try do
      assert :ok == execute(ctx.worker, child)

      assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :pulls, 1, Access.key(:state)]) ==
               :closed
    after
      :sys.resume(GitHub.Advisory)
    end
  end

  test "cleanup preserves a prefixed title and independently records a human-closed PR", ctx do
    {_source, _patches} = completed_cleanup(ctx, 2)
    [one, two] = cleanup_children(ctx)
    mock = GitHub.ServerMock.get_state()
    mock = put_in(mock, [ctx.conn, :pulls, 1, Access.key(:title)], "[Merged by Bors] - Title")
    mock = put_in(mock, [ctx.conn, :pulls, 2, Access.key(:state)], :closed)
    GitHub.ServerMock.put_state(mock)
    assert :ok == execute(ctx.worker, one)
    assert :ok == execute(ctx.worker, two)
    mock = GitHub.ServerMock.get_state()
    assert get_in(mock, [ctx.conn, :pulls, 1, Access.key(:title)]) == "[Merged by Bors] - Title"
    assert get_in(mock, [ctx.conn, :pulls, 2, Access.key(:title)]) == "Title"

    notices =
      Repo.all(
        from(j in OrchestrationJob, where: j.project_id == ^ctx.project.id and j.kind == "notice")
      )

    assert length(notices) == 2
    assert Enum.any?(notices, fn j -> match?({_, 1, _, _}, Outbox.payload(j)) end)
  end

  test "permanent cleanup failures retain a visible tombstone and release siblings", ctx do
    {_source, _patches} = completed_cleanup(ctx, 2)
    [one, two] = cleanup_children(ctx)
    GitHub.ServerMock.put_state(Map.put(GitHub.ServerMock.get_state(), :close_pr_status, 422))
    assert {:terminal, {:http_status, 422}} == execute(ctx.worker, one)
    done = Repo.get!(OrchestrationJob, one.id)
    assert done.state == "done"
    assert done.dedup_key == one.dedup_key
    assert String.starts_with?(done.last_error, "cleanup_terminal:")
    assert :ok == execute(ctx.worker, two)
  end

  for {content, closes} <- [{nil, false}, {"status = [\"ci\"]\nuse_squash_merge = true", true}] do
    test "legacy cleanup resolves bounded .github policy (squash=#{closes})", ctx do
      files = if unquote(content), do: %{".github/bors.toml" => unquote(content)}, else: %{}
      mock = put_in(GitHub.ServerMock.get_state(), [ctx.conn, :files, "completed"], files)
      GitHub.ServerMock.put_state(mock)
      {_source, _patches} = completed_cleanup(ctx, 1, nil)
      assert length(cleanup_children(ctx)) == if(unquote(closes), do: 1, else: 0)
    end
  end

  test "finishing an owned cleanup tombstone does not deserialize its payload", ctx do
    job = Outbox.enqueue(ctx.project.id, "info", :poll, "post-merge:invalid-fixture")
    {:ok, claimed} = Outbox.claim(job.id)
    claimed |> Ecto.Changeset.change(payload: <<0>>) |> Repo.update!()
    claimed = Repo.get!(OrchestrationJob, claimed.id)
    assert {1, _} = Outbox.finish(claimed)
    assert Repo.get!(OrchestrationJob, claimed.id).dedup_key == job.dedup_key
  end

  test "terminal completion yields before preparing the next waiting batch", ctx do
    dispatch = construct(ctx)
    assert :ok == execute(ctx.worker, dispatch)
    batch = Repo.get!(Batch, ctx.batch.id)
    waiting = Repo.insert!(%Batch{Batch.new(ctx.project.id, "master") | last_polled: 0})
    failed_poll = Outbox.enqueue(ctx.project.id, "info", {:poll, :once})
    {:ok, failed_poll} = Outbox.claim(failed_poll.id)
    Outbox.fail(failed_poll, :infrastructure)

    GitHub.ServerMock.put_state(
      put_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, batch.commit], %{
        "ci" => :ok,
        "cd" => :ok
      })
    )

    assert :ok ==
             execute(
               ctx.worker,
               Outbox.enqueue(ctx.project.id, "cast", {:status, {batch.commit, "ci", :ok, "url"}})
             )

    assert Repo.get!(Batch, batch.id).state == :ok

    assert get_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, batch.commit, "bors"]) ==
             :ok

    refute get_in(GitHub.ServerMock.get_state(), [ctx.conn, :statuses, "N", "bors"]) == :ok
    assert Repo.get!(Batch, waiting.id).state == :waiting
    assert Outbox.pending?(ctx.project.id, "complete:#{batch.id}")
    assert Enum.any?(Outbox.due(32), &(Outbox.payload(&1) == {:completion_poll, batch.id}))
    assert Repo.get!(OrchestrationJob, failed_poll.id).available_at > Outbox.now()
  end

  test "one recovery turn handles one PR and durably yields the remaining bounded budget", ctx do
    patches =
      for n <- 1..4 do
        Repo.insert!(%Patch{
          project_id: ctx.project.id,
          pr_xref: 100 + n,
          commit: "H#{n}",
          into_branch: "main",
          queue_pending: true,
          queue_approval: %{
            "state" => "approved",
            "head_sha" => "H#{n}",
            "reviewer" => "reviewer"
          }
        })
      end

    job = Outbox.enqueue(ctx.project.id, "info", :recover_queue)
    assert :ok == execute(ctx.worker, job)
    {:dictionary, data} = Process.info(ctx.worker, :dictionary)
    assert Map.new(data)[:queue_recovery_cursor] == hd(patches).id

    Enum.each(3..1//-1, fn budget ->
      # A restart loses the process cursor; the durable visited list still
      # advances through distinct PRs instead of terminating on the first one.
      :sys.replace_state(ctx.worker, fn state ->
        Process.delete(:queue_recovery_cursor)
        state
      end)

      [slice] =
        Repo.all(
          from(j in OrchestrationJob,
            where: j.project_id == ^ctx.project.id and j.state == "pending"
          )
        )
        |> Enum.filter(&match?({:recover_queue_slice, ^budget, _}, Outbox.payload(&1)))

      assert :ok == execute(ctx.worker, slice)
    end)

    {:dictionary, data} = Process.info(ctx.worker, :dictionary)
    assert Map.new(data)[:queue_recovery_cursor] == List.last(patches).id

    refute Enum.any?(
             Repo.all(
               from(j in OrchestrationJob,
                 where: j.project_id == ^ctx.project.id and j.state == "pending"
               )
             ),
             fn j -> match?({:recover_queue_slice, _, _}, Outbox.payload(j)) end
           )
  end

  test "a short recovery queue is visited only once per original wakeup", ctx do
    patch =
      Repo.insert!(%Patch{
        project_id: ctx.project.id,
        pr_xref: 999,
        commit: "H",
        into_branch: "main",
        queue_pending: true,
        queue_approval: %{"state" => "approved", "head_sha" => "H", "reviewer" => "reviewer"}
      })

    assert :ok == execute(ctx.worker, Outbox.enqueue(ctx.project.id, "info", :recover_queue))

    [slice] =
      Repo.all(
        from(j in OrchestrationJob,
          where: j.project_id == ^ctx.project.id and j.state == "pending"
        )
      )
      |> Enum.filter(&match?({:recover_queue_slice, _, _}, Outbox.payload(&1)))

    assert {:recover_queue_slice, 3, [id]} = Outbox.payload(slice)
    assert id == patch.id
    assert :ok == execute(ctx.worker, slice)

    refute Enum.any?(
             Repo.all(
               from(j in OrchestrationJob,
                 where: j.project_id == ^ctx.project.id and j.state == "pending"
               )
             ),
             &match?({:recover_queue_slice, _, _}, Outbox.payload(&1))
           )
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
    # The newly persisted priority command fences publication until processed.
    assert_receive {:finished, worker, _, {:defer, :withdrawal_pending}}, 5000
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
