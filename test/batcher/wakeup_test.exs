defmodule BorsNG.Worker.WakeupTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{OrchestrationJob, Patch, Project, Repo}
  alias BorsNG.Worker.Orchestration.Outbox

  setup do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    project = Repo.insert!(%Project{name: "wake-test", repo_xref: 4811})
    patch = Repo.insert!(%Patch{project_id: project.id, pr_xref: 1, commit: "head"})
    on_exit(fn -> :sys.resume(BorsNG.Worker.Orchestration.Dispatcher) end)
    {:ok, project: project, patch: patch}
  end

  test "one pending eligibility read covers every receipt before claim", %{project: p} do
    jobs = Enum.map(1..40, fn _ -> Outbox.enqueue(p.id, "cast", {:eligibility, 1}) end)
    assert length(Enum.uniq_by(jobs, & &1.id)) == 1
    assert List.last(jobs).wake_version == 40
    {:ok, claimed} = Outbox.claim(hd(jobs).id)
    assert claimed.claimed_version == 40
    assert {1, _} = Outbox.finish(claimed)
    assert Repo.get!(OrchestrationJob, claimed.id).state == "done"
    # Completion clears the key, so a later request gets a fresh read.
    assert Outbox.enqueue(p.id, "cast", {:eligibility, 1}).id != claimed.id
  end

  test "future poll deadlines survive an earlier pending or running poll", %{project: p} do
    job = Outbox.enqueue(p.id, "info", {:poll, :once})
    early = Outbox.enqueue(p.id, "info", {:poll, :once}, "timer:10", Outbox.now() + 10)
    {:ok, claimed} = Outbox.claim(job.id)
    late = Outbox.enqueue(p.id, "info", {:poll, :once}, "timer:30", Outbox.now() + 30)
    assert early.id != job.id and late.id != job.id
    assert early.wake_version == 0 and late.wake_version == 0
    assert {1, _} = Outbox.finish(claimed)
    assert Repo.get!(OrchestrationJob, early.id).state == "pending"
    assert Repo.get!(OrchestrationJob, late.id).state == "pending"
    assert early.available_at < late.available_at
    early |> Ecto.Changeset.change(available_at: 0) |> Repo.update!()
    {:ok, early_claim} = Outbox.claim(early.id)
    Outbox.finish(early_claim)
    assert Repo.get!(OrchestrationJob, late.id).state == "pending"
    assert Repo.get!(OrchestrationJob, late.id).available_at == late.available_at
  end

  test "a receipt during a read survives after intervening commands in a new FIFO row", %{
    project: p,
    patch: patch
  } do
    job = Outbox.enqueue(p.id, "cast", {:eligibility, 1})
    {:ok, claimed} = Outbox.claim(job.id)
    command = Outbox.enqueue(p.id, "cast", {:set_priority, patch.id, 7})
    receipt = Outbox.enqueue(p.id, "cast", {:eligibility, 1})
    assert receipt.id == job.id
    assert {1, _} = Outbox.finish(claimed)
    assert Repo.get!(OrchestrationJob, job.id).state == "done"
    followup = Repo.get_by!(OrchestrationJob, dedup_key: "wake:#{p.id}:eligibility:1")
    assert followup.id > command.id and followup.wake_version == 1
    assert followup.enqueued_at_ms >= job.enqueued_at_ms
    assert Enum.map(Outbox.due(32), & &1.id) == [command.id, followup.id]
    {:ok, command_claim} = Outbox.claim(command.id)
    Outbox.finish(command_claim)
    {:ok, current} = Outbox.claim(followup.id)
    assert {0, _} = Outbox.finish(claimed)
    assert {1, _} = Outbox.finish(current)
  end

  test "a pending coalesced read does not swallow a wakeup after an intervening command", %{
    project: p,
    patch: patch
  } do
    first = Outbox.enqueue(p.id, "cast", {:eligibility, 1})
    command = Outbox.enqueue(p.id, "cast", {:set_priority, patch.id, 7})
    later = Outbox.enqueue(p.id, "cast", {:eligibility, 1})
    assert first.id < command.id and later.id > command.id
    assert Repo.get!(OrchestrationJob, first.id).dedup_key == nil
    assert Enum.map(Outbox.due(32), & &1.id) == [first.id, command.id, later.id]
  end

  test "an initially unknown PR resolves its subject before a later FIFO continuation", %{
    project: p
  } do
    first = Outbox.enqueue(p.id, "cast", {:eligibility, 99})
    assert first.subject_id == 0
    {:ok, claimed} = Outbox.claim(first.id)
    patch = Repo.insert!(%Patch{project_id: p.id, pr_xref: 99, commit: "new"})
    command = Outbox.enqueue(p.id, "cast", {:set_priority, patch.id, 7})
    Outbox.enqueue(p.id, "cast", {:eligibility, 99})
    Outbox.finish(claimed)
    next = Repo.get_by!(OrchestrationJob, dedup_key: "wake:#{p.id}:eligibility:99")
    assert next.subject_id == patch.id and next.priority == 2 and next.id > command.id
    assert Enum.map(Outbox.due(32), & &1.id) == [command.id, next.id]
  end

  test "new wakeups cannot shorten infrastructure backoff", %{project: p} do
    job = Outbox.enqueue(p.id, "cast", {:eligibility, 2})
    {:ok, claimed} = Outbox.claim(job.id)
    assert {1, _} = Outbox.fail(claimed, :unavailable)
    deadline = Repo.get!(OrchestrationJob, job.id).available_at
    again = Outbox.enqueue(p.id, "cast", {:eligibility, 2})
    assert again.id == job.id and again.wake_version == 2
    assert again.available_at == deadline
    assert {:ok, nil} == Outbox.claim(job.id)
  end

  test "dirty revision and lease ownership survive restart recovery", %{project: p} do
    job = Outbox.enqueue(p.id, "info", :recover_queue)
    {:ok, old} = Outbox.claim(job.id)
    again = Outbox.enqueue(p.id, "info", :recover_queue)
    assert again.wake_version == 2
    Outbox.recover_expired()
    assert Repo.get!(OrchestrationJob, job.id).state == "running"
    Repo.get!(OrchestrationJob, job.id) |> Ecto.Changeset.change(lease_until: 0) |> Repo.update!()
    Outbox.recover_expired()
    {:ok, current} = Outbox.claim(job.id)
    assert current.token != old.token and current.claimed_version == 2
    assert {0, _} = Outbox.finish(old)
    assert {1, _} = Outbox.finish(current)
    assert Repo.get!(OrchestrationJob, job.id).state == "done"
  end

  test "eligibility keys separate PRs, while exact commands and observations remain distinct", %{
    project: p
  } do
    a = Outbox.enqueue(p.id, "cast", {:eligibility, 3})
    b = Outbox.enqueue(p.id, "cast", {:eligibility, 4})
    assert a.id != b.id

    commands =
      for _ <- 1..2,
          do: Outbox.enqueue(p.id, "cast", {:reviewed_generation, 1, "head", "token", "reviewer"})

    withdrawals =
      for token <- ["old", "new"],
          do: Outbox.enqueue(p.id, "cast", {:cancel_generation, 1, "head", token, :requested})

    observations =
      for verdict <- [:ok, :error, :waiting],
          do: Outbox.enqueue(p.id, "cast", {:status, {"head", "ci", verdict, "url"}})

    all = commands ++ withdrawals ++ observations
    assert length(Enum.uniq_by(all, & &1.id)) == 7
    assert Enum.all?(all, &(&1.wake_version == 0))
  end

  test "recovery and held-backend wakeups each have their own durable dirty key", %{project: p} do
    a = Outbox.enqueue(p.id, "info", :recover_queue)
    b = Outbox.enqueue(p.id, "info", :wake_backend_holds)
    assert a.id != b.id
    assert Outbox.enqueue(p.id, "info", :recover_queue).id == a.id
    assert Outbox.enqueue(p.id, "info", :wake_backend_holds).id == b.id
  end
end
