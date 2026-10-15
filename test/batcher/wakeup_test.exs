defmodule BorsNG.Worker.WakeupTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{OrchestrationJob, Project, Repo}
  alias BorsNG.Worker.Orchestration.Outbox

  setup do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    project = Repo.insert!(%Project{name: "wake-test", repo_xref: 4811})
    on_exit(fn -> :sys.resume(BorsNG.Worker.Orchestration.Dispatcher) end)
    {:ok, project: project}
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

  test "arrival during a read guarantees one follow-up, with the earliest requested deadline", %{
    project: p
  } do
    job = Outbox.enqueue(p.id, "info", {:poll, :once})
    {:ok, claimed} = Outbox.claim(job.id)
    first = Outbox.enqueue(p.id, "info", {:poll, :once}, nil, Outbox.now() + 30)
    second = Outbox.enqueue(p.id, "info", {:poll, :once}, nil, Outbox.now() + 10)
    assert first.id == job.id and second.id == job.id
    assert {1, _} = Outbox.finish(claimed)
    dirty = Repo.get!(OrchestrationJob, job.id)
    assert dirty.state == "pending" and dirty.wake_version == 3
    assert dirty.available_at == second.rerun_at
    assert dirty.available_at >= Outbox.now() + 9
    assert {:ok, nil} == Outbox.claim(job.id)
    dirty |> Ecto.Changeset.change(available_at: 0) |> Repo.update!()
    {:ok, followup} = Outbox.claim(job.id)
    assert followup.claimed_version == 3
    assert {0, _} = Outbox.finish(claimed)
    assert {1, _} = Outbox.finish(followup)
    assert Repo.get!(OrchestrationJob, job.id).state == "done"
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
