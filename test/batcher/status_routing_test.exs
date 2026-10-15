defmodule BorsNG.Worker.StatusRoutingTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Attempt, Batch, Patch, Project, Repo}
  alias BorsNG.Worker.StatusRouting

  setup do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    project = Repo.insert!(%Project{name: "status-test", repo_xref: 4812})

    on_exit(fn ->
      System.delete_env("BORS_ASYNC_ORCHESTRATION")
      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end)

    {:ok, project: project}
  end

  test "unmatched ordinary PR/self status traffic cannot occupy the async lane", %{project: p} do
    Repo.insert!(%Patch{project_id: p.id, commit: "ordinary-pr", pr_xref: 1})
    refute StatusRouting.batch?(p.id, "ordinary-pr")
    refute StatusRouting.batch?(p.id, "unknown-head")
  end

  test "any recorded candidate commit, including a terminal generation, is retained", %{
    project: p
  } do
    Repo.insert!(%Batch{project_id: p.id, state: :running, commit: "running-head"})
    Repo.insert!(%Batch{project_id: p.id, state: :canceled, commit: "old-head"})
    assert StatusRouting.batch?(p.id, "running-head")
    assert StatusRouting.batch?(p.id, "old-head")
  end

  test "candidate assembly before binding conservatively retains unknown receipts", %{project: p} do
    batch = Repo.insert!(Batch.new(p.id, "master"))
    BorsNG.Worker.CandidateJournal.begin(batch, [], "base")
    assert StatusRouting.batch?(p.id, "not-yet-bound")
  end

  test "trying identities are retained, scoped to their repository", %{project: p} do
    patch = Repo.insert!(%Patch{project_id: p.id, commit: "pr", pr_xref: 1})
    Repo.insert!(%Attempt{patch_id: patch.id, state: 1, commit: "try-head"})
    assert StatusRouting.batch?(p.id, "try-head")
    other = Repo.insert!(%Project{name: "other", repo_xref: 4813})
    refute StatusRouting.batch?(other.id, "try-head")
  end

  test "legacy publication-before-record delivery remains unchanged", %{project: p} do
    System.delete_env("BORS_ASYNC_ORCHESTRATION")
    assert StatusRouting.batch?(p.id, "not-yet-recorded-legacy-head")
  end
end
