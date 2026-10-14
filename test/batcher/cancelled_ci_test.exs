defmodule BorsNG.Worker.BatcherCancelledCITest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Batch, Installation, LinkPatchBatch, Patch, Project, Repo, Status}
  alias BorsNG.Worker.Batcher
  alias BorsNG.GitHub

  setup do
    old = System.get_env("BORS_STAGE_DISPATCH_PROJECT")
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", "project_name")

    on_exit(fn ->
      if old,
        do: System.put_env("BORS_STAGE_DISPATCH_PROJECT", old),
        else: System.delete_env("BORS_STAGE_DISPATCH_PROJECT")
    end)

    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "project_name",
        installation_id: inst.id,
        repo_xref: 14,
        staging_branch: "staging"
      })

    approval = %{"state" => "approved", "head_sha" => "N", "reviewer" => "rvr"}

    patch =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: 1,
        commit: "N",
        open: true,
        into_branch: "master",
        queue_approval: approval,
        queue_pending: true
      })

    batch =
      Repo.insert!(%Batch{
        project_id: project.id,
        into_branch: "master",
        state: :running,
        commit: "staged",
        base_sha: "base",
        last_polled: 0,
        timeout_at: DateTime.to_unix(DateTime.utc_now()) + 3_600
      })

    link =
      Repo.insert!(%LinkPatchBatch{
        batch_id: batch.id,
        patch_id: patch.id,
        reviewer: "rvr",
        head_sha: "N"
      })

    Repo.insert!(%Status{batch_id: batch.id, identifier: "build", state: :error})

    GitHub.ServerMock.put_state(%{
      {{:installation, 91}, 14} => %{
        statuses: %{"staged" => %{"build" => :error}},
        staging_failures: %{"staged" => :cancelled},
        comments: %{1 => []},
        labels: %{1 => []},
        files: %{"master" => %{"bors.toml" => ~s/status = [ "build" ]/}}
      }
    })

    {:ok, project: project, patch: patch, batch: batch, link: link}
  end

  defp active_child(project) do
    [batch] = Repo.all(Batch.all_for_project(project.id, :incomplete))
    batch
  end

  test "cancelled singleton retains exact-head intent and membership", ctx do
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    batch = active_child(ctx.project)
    patch = Repo.get!(Patch, ctx.patch.id)
    assert batch.state == :waiting
    assert batch.sealed
    {normal, true} = Batcher.get_new_batch(nil, ctx.project.id, "master", batch.priority, false)
    refute normal.sealed
    assert normal.id != batch.id
    Repo.delete!(normal)
    assert batch.commit == nil and batch.base_sha == nil and batch.timeout_at in [nil, 0]
    assert batch.recovery_failures == 1
    assert batch.recovery_after > DateTime.to_unix(DateTime.utc_now())
    assert Repo.all(Status.all_for_batch(batch.id)) == []
    assert patch.queue_approval == ctx.patch.queue_approval and patch.queue_pending
    assert Repo.get!(LinkPatchBatch, ctx.link.id).head_sha == "N"
    assert Repo.all(Batch.all_for_project(ctx.project.id)) |> length() == 2
    statuses = GitHub.ServerMock.get_state()[{{:installation, 91}, 14}][:statuses]
    assert statuses["N"]["bors"] == :running

    # A duplicate completion or poll during backoff does not create more work.
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, batch.id).recovery_failures == 1
  end

  test "trusted infrastructure evidence survives native retry without blaming a singleton", ctx do
    alias BorsNG.Worker.CandidateJournal

    candidate =
      CandidateJournal.begin(ctx.batch, Repo.all(LinkPatchBatch.from_batch(ctx.batch.id)), "base")

    CandidateJournal.bind(
      candidate,
      "staged",
      "tree",
      "base",
      %BorsNG.Worker.Batcher.BorsToml{status: ["build"]},
      %{content: nil}
    )

    state = GitHub.ServerMock.get_state()

    evidence = %{
      run_id: 123,
      run_attempt: 2,
      conclusion: "failure",
      failed_steps: [%{name: "isolated publisher"}]
    }

    GitHub.ServerMock.put_state(
      put_in(
        state,
        [{{:installation, 91}, 14}, :staging_failures, "staged"],
        {:infrastructure, evidence}
      )
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    assert active_child(ctx.project).sealed
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval

    events =
      Repo.all(from(e in BorsNG.Database.CandidateEvent, where: e.candidate_id == ^candidate.id))

    assert Enum.any?(
             events,
             &(&1.kind == "failure_classified" and
                 &1.details["classification"] == "infrastructure" and
                 &1.details["evidence"]["run_id"] == 123)
           )

    refute Enum.any?(events, &(&1.kind == "head_quarantined"))
  end

  test "unavailable classification holds CI instead of failing approval", ctx do
    state = GitHub.ServerMock.get_state()
    state = put_in(state, [{{:installation, 91}, 14}, :staging_failures, "staged"], :unavailable)
    GitHub.ServerMock.put_state(state)
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running and batch.commit == "staged"
    assert batch.timeout_at == ctx.batch.timeout_at
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
    assert Repo.get!(LinkPatchBatch, ctx.link.id)
  end

  test "withdrawal during backoff remains withdrawn", ctx do
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    child = active_child(ctx.project)
    Batcher.handle_cast({:cancel, ctx.patch.id, :requested}, ctx.project.id)
    patch = Repo.get!(Patch, ctx.patch.id)
    assert patch.queue_approval["state"] == "withdrawn"
    refute patch.queue_pending
    assert Repo.all(LinkPatchBatch.from_batch(child.id)) == []
  end

  test "a conclusive failed singleton still loses approval", ctx do
    state = GitHub.ServerMock.get_state()
    state = put_in(state, [{{:installation, 91}, 14}, :staging_failures, "staged"], :conclusive)
    GitHub.ServerMock.put_state(state)
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :error
    assert Repo.get!(Patch, ctx.patch.id).queue_approval["state"] == "failed"
    refute Repo.get!(Patch, ctx.patch.id).queue_pending
  end

  test "in-progress finalizer propagation is a short hold without failure charges", ctx do
    state = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      put_in(state, [{{:installation, 91}, 14}, :staging_failures, "staged"], :pending)
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running
    assert batch.recovery_failures == 0
    assert batch.timeout_at == ctx.batch.timeout_at
    assert batch.recovery_after <= DateTime.to_unix(DateTime.utc_now()) + 60
    statuses = GitHub.ServerMock.get_state()[{{:installation, 91}, 14}][:statuses]
    refute statuses["staged"]["bors"] == :ok
  end

  test "early scope and bump-guard failures are classified before bisection", ctx do
    for guard <- ["scope", "bump-guard"] do
      batch = active_child(ctx.project)
      Repo.delete_all(Status.all_for_batch(batch.id))
      Repo.insert!(%Status{batch_id: batch.id, identifier: guard, state: :error})
      Repo.insert!(%Status{batch_id: batch.id, identifier: "build", state: :running})

      batch
      |> Batch.changeset(%{state: :running, commit: "staged", last_polled: 0, recovery_after: 0})
      |> Repo.update!()

      state = GitHub.ServerMock.get_state()

      GitHub.ServerMock.put_state(
        put_in(state, [{{:installation, 91}, 14}, :statuses, "staged"], %{
          guard => :error,
          "build" => :running
        })
      )

      Batcher.handle_info({:poll, :once}, ctx.project.id)
      assert active_child(ctx.project).state == :waiting
      assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
    end
  end

  test "unverifiable evidence releases staging after bounded observations", ctx do
    state = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      put_in(state, [{{:installation, 91}, 14}, :staging_failures, "staged"], :unavailable)
    )

    for count <- 1..3 do
      batch = Repo.get!(Batch, ctx.batch.id)
      batch |> Batch.changeset(%{last_polled: 0, recovery_after: 0}) |> Repo.update!()
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      assert Repo.get!(Batch, batch.id).recovery_failures == count
    end

    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :canceled
    assert active_child(ctx.project).state == :waiting
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
  end

  test "expired CI cannot bypass cancellation classification", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    assert active_child(ctx.project).state == :waiting
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
  end

  test "pending result at the deadline releases staging without a failed approval", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    state = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      put_in(state, [{{:installation, 91}, 14}, :staging_failures, "staged"], :pending)
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    assert active_child(ctx.project).state == :waiting
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
  end

  test "missing terminal CI at the deadline retains intent and backs off", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    Repo.update_all(Status.all_for_batch(ctx.batch.id), set: [state: :running])
    state = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      put_in(state, [{{:installation, 91}, 14}, :statuses, "staged"], %{})
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    child = active_child(ctx.project)
    assert child.state == :waiting and child.recovery_failures == 1
    assert child.recovery_after >= DateTime.to_unix(DateTime.utc_now()) + 299
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
    assert Repo.all(LinkPatchBatch.from_batch(child.id)) |> length() == 1

    # Duplicate polls during durable backoff must not spend another API read.
    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :get_commit_status_error, 1)
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert GitHub.ServerMock.get_state()[:get_commit_status_error] == 1
    assert active_child(ctx.project).id == child.id
  end

  test "a status API outage at the CI deadline cannot revoke approval", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    Repo.update_all(Status.all_for_batch(ctx.batch.id), set: [state: :running])

    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :get_commit_status_error, 1)
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    assert active_child(ctx.project).state == :waiting
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
  end

  test "an ordinary project still handles its CI deadline as a timeout", ctx do
    System.delete_env("BORS_STAGE_DISPATCH_PROJECT")

    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    Repo.update_all(Status.all_for_batch(ctx.batch.id), set: [state: :running])
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :error
    assert Repo.get!(Patch, ctx.patch.id).queue_approval["reason"] == "timeout"
  end

  test "success arriving at the deadline supersedes cached pending CI", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    Repo.update_all(Status.all_for_batch(ctx.batch.id), set: [state: :running])
    state = GitHub.ServerMock.get_state()
    repo = state[{{:installation, 91}, 14}]

    repo =
      repo
      |> Map.put(:branches, %{"master" => "base"})
      |> put_in([:statuses, "staged"], %{"build" => :ok})
      |> put_in([:files, "staging"], %{"bors.toml" => ~s/status = [ "build" ]/})

    GitHub.ServerMock.put_state(Map.put(state, {{:installation, 91}, 14}, repo))
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :ok
    assert Repo.get!(Batch, ctx.batch.id).recovery_failures == 0
    assert Repo.all(Batch.all_for_project(ctx.project.id)) |> length() == 1
  end

  test "a conclusive failure arriving at the deadline still fails the singleton", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    Repo.update_all(Status.all_for_batch(ctx.batch.id), set: [state: :running])
    state = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      put_in(state, [{{:installation, 91}, 14}, :staging_failures, "staged"], :conclusive)
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :error
    assert Repo.get!(Patch, ctx.patch.id).queue_approval["state"] == "failed"
    assert Repo.all(Batch.all_for_project(ctx.project.id)) |> length() == 1
  end

  test "an obsolete failure webhook at the deadline cannot override newer success", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    state = GitHub.ServerMock.get_state()

    repo =
      state[{{:installation, 91}, 14}]
      |> Map.put(:branches, %{"master" => "base"})
      |> put_in([:statuses, "staged"], %{"build" => :ok})
      |> put_in([:files, "staging"], %{"bors.toml" => ~s/status = [ "build" ]/})

    GitHub.ServerMock.put_state(Map.put(state, {{:installation, 91}, 14}, repo))
    Batcher.handle_cast({:status, {"staged", "build", :error, "old-run"}}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :ok
    assert Repo.get!(Batch, ctx.batch.id).recovery_failures == 0
    assert Repo.all(Batch.all_for_project(ctx.project.id)) |> length() == 1
  end

  test "an API outage does not discard green CI held for promotion", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    Repo.update_all(Status.all_for_batch(ctx.batch.id), set: [state: :ok])

    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :get_commit_status_error, 1)
    )

    Batcher.handle_info({:retry_promotion, ctx.batch.id, ctx.batch.commit}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running and batch.commit == ctx.batch.commit
    assert batch.recovery_failures == 0
    assert Repo.all(Batch.all_for_project(ctx.project.id)) |> length() == 1
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
    timer = Process.get({:batch_recheck, ctx.batch.id})
    assert is_reference(timer) and Process.read_timer(timer) <= 61_000
  end

  test "repeated missing CI uses the cancellation backoff and respects withdrawal", ctx do
    for count <- 1..6 do
      batch = active_child(ctx.project)

      batch
      |> Batch.changeset(%{
        state: :running,
        commit: "staged",
        base_sha: "base",
        last_polled: 0,
        recovery_after: 0,
        timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1
      })
      |> Repo.update!()

      Repo.delete_all(Status.all_for_batch(batch.id))
      Repo.insert!(%Status{batch_id: batch.id, identifier: "build", state: :running})
      state = GitHub.ServerMock.get_state()

      GitHub.ServerMock.put_state(
        put_in(state, [{{:installation, 91}, 14}, :statuses, "staged"], %{})
      )

      Batcher.handle_info({:poll, :once}, ctx.project.id)
      child = active_child(ctx.project)
      assert child.recovery_failures == count
      delay = min(300 * Integer.pow(2, min(count - 1, 4)), 3_600)
      assert child.recovery_after >= DateTime.to_unix(DateTime.utc_now()) + delay - 1
      assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
      assert Repo.all(LinkPatchBatch.from_batch(child.id)) |> length() == 1
    end

    child = active_child(ctx.project)
    Batcher.handle_cast({:cancel, ctx.patch.id, :requested}, ctx.project.id)
    Batcher.handle_cast({:status, {"staged", "build", :error, "old-run"}}, ctx.project.id)
    assert Repo.get!(Patch, ctx.patch.id).queue_approval["state"] == "withdrawn"
    refute Repo.get!(Patch, ctx.patch.id).queue_pending
    assert Repo.all(LinkPatchBatch.from_batch(child.id)) == []
  end

  test "repeated cancellation preserves backoff and one active membership", ctx do
    for count <- 1..6 do
      batch = active_child(ctx.project)

      batch
      |> Batch.changeset(%{
        state: :running,
        commit: "staged",
        base_sha: "base",
        last_polled: 0,
        recovery_after: 0,
        timeout_at: DateTime.to_unix(DateTime.utc_now()) + 10
      })
      |> Repo.update!()

      Repo.delete_all(Status.all_for_batch(batch.id))
      Repo.insert!(%Status{batch_id: batch.id, identifier: "build", state: :error})
      Batcher.handle_info({:poll, :once}, ctx.project.id)
      assert Repo.get!(Batch, batch.id).state == :canceled
      observed = active_child(ctx.project)
      assert observed.recovery_failures == count
      delay = min(300 * Integer.pow(2, min(count - 1, 4)), 3_600)
      assert observed.recovery_after >= DateTime.to_unix(DateTime.utc_now()) + delay - 1
      assert observed.state == :waiting
      assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
      assert Repo.all(LinkPatchBatch.from_batch(observed.id)) |> length() == 1
    end
  end

  test "a due recheck bypasses the coarse poll and resolves genuine failure", ctx do
    state = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      put_in(state, [{{:installation, 91}, 14}, :staging_failures, "staged"], :pending)
    )

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    timer = Process.get({:batch_recheck, ctx.batch.id})
    assert is_reference(timer)
    assert Process.read_timer(timer) <= 61_000
    batch = Repo.get!(Batch, ctx.batch.id)
    refute Batch.next_poll_is_past(Repo.preload(batch, :project))
    batch |> Batch.changeset(%{recovery_after: 0}) |> Repo.update!()
    state = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      put_in(state, [{{:installation, 91}, 14}, :staging_failures, "staged"], :conclusive)
    )

    Batcher.handle_info({:retry_promotion, batch.id, batch.commit}, ctx.project.id)
    assert Repo.get!(Batch, batch.id).state == :error
    assert Repo.get!(Patch, ctx.patch.id).queue_approval["state"] == "failed"
    assert Process.get({:batch_recheck, batch.id}) == nil
  end

  test "old retry timer and status do not reactivate a cancelled attempt", ctx do
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    child = active_child(ctx.project)
    Batcher.handle_info({:retry_promotion, ctx.batch.id, "staged"}, ctx.project.id)
    Batcher.handle_cast({:status, {"staged", "build", :error, "old-run"}}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    assert active_child(ctx.project).id == child.id
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
  end

  test "timer restoration reads a running hold's durable deadline", ctx do
    deadline = DateTime.to_unix(DateTime.utc_now()) + 60
    ctx.batch |> Batch.changeset(%{recovery_after: deadline}) |> Repo.update!()
    # Exercise the boot message without registering this ExUnit process as
    # a real batcher in the application's shared registry.
    Batcher.handle_info(:restore_rechecks, ctx.project.id)
    timer = Process.get({:batch_recheck, ctx.batch.id})
    assert is_reference(timer)
    assert Process.read_timer(timer) <= 61_000
    Process.cancel_timer(timer)
  end

  test "a failed status read from the hold timer keeps a prompt recheck", ctx do
    GitHub.ServerMock.put_state(
      Map.put(GitHub.ServerMock.get_state(), :get_commit_status_error, 1)
    )

    Batcher.handle_info({:retry_promotion, ctx.batch.id, ctx.batch.commit}, ctx.project.id)
    timer = Process.get({:batch_recheck, ctx.batch.id})
    assert is_reference(timer)
    assert Process.read_timer(timer) <= 61_000
    assert Repo.get!(Batch, ctx.batch.id).state == :running
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
  end
end
