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

  test "cancelled singleton retains exact-head intent and membership", ctx do
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    patch = Repo.get!(Patch, ctx.patch.id)
    assert batch.state == :waiting
    assert batch.commit == nil and batch.base_sha == nil and batch.timeout_at == 0
    assert batch.recovery_failures == 1
    assert batch.recovery_after > DateTime.to_unix(DateTime.utc_now())
    assert Repo.all(Status.all_for_batch(batch.id)) == []
    assert patch.queue_approval == ctx.patch.queue_approval and patch.queue_pending
    assert Repo.get!(LinkPatchBatch, ctx.link.id).head_sha == "N"
    assert Repo.all(Batch.all_for_project(ctx.project.id)) |> length() == 1

    # A duplicate completion or poll during backoff does not create more work.
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, batch.id).recovery_failures == 1
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
    Batcher.handle_cast({:cancel, ctx.patch.id, :requested}, ctx.project.id)
    patch = Repo.get!(Patch, ctx.patch.id)
    assert patch.queue_approval["state"] == "withdrawn"
    refute patch.queue_pending
    assert Repo.get(LinkPatchBatch, ctx.link.id) == nil
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
      Repo.delete_all(Status.all_for_batch(ctx.batch.id))
      Repo.insert!(%Status{batch_id: ctx.batch.id, identifier: guard, state: :error})
      Repo.insert!(%Status{batch_id: ctx.batch.id, identifier: "build", state: :running})

      ctx.batch
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
      assert Repo.get!(Batch, ctx.batch.id).state == :waiting
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
    assert batch.state == :waiting and batch.commit == nil
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
  end

  test "expired CI cannot bypass cancellation classification", ctx do
    ctx.batch
    |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) - 1})
    |> Repo.update!()

    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, ctx.batch.id).state == :waiting
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
    assert Repo.get!(Batch, ctx.batch.id).state == :waiting
    assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
  end

  test "repeated cancellation persists exponential backoff without new memberships", ctx do
    for count <- 1..6 do
      batch = Repo.get!(Batch, ctx.batch.id)

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
      observed = Repo.get!(Batch, batch.id)
      assert observed.recovery_failures == count
      delay = min(300 * Integer.pow(2, min(count - 1, 4)), 3_600)
      assert observed.recovery_after >= DateTime.to_unix(DateTime.utc_now()) + delay - 1
      assert observed.state == :waiting
      assert Repo.get!(Patch, ctx.patch.id).queue_approval == ctx.patch.queue_approval
      assert Repo.all(LinkPatchBatch.from_batch(batch.id)) |> length() == 1
    end
  end
end
