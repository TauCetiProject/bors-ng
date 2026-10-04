defmodule BorsNG.MergeBackendTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Batch, Installation, Project, Repo}
  alias BorsNG.{GitHub, MergeBackend}
  alias BorsNG.Worker.Batcher

  test "incoming admissions wait, existing batches drain with either selection" do
    for mode <- ["queue", "bors"] do
      snapshot = {:ok, %{backend: mode, github_count: 0}}
      assert MergeBackend.decide(snapshot, :start) == :ok
      assert MergeBackend.decide(snapshot, :admit) == :ok == (mode == "bors")

      assert {:defer, :github_not_drained} =
               MergeBackend.decide({:ok, %{backend: mode, github_count: 2}}, :start)
    end

    assert {:defer, _} = MergeBackend.decide({:error, :unauthorized}, :admit)

    assert {:defer, _} =
             MergeBackend.decide({:ok, %{backend: "invalid", github_count: 0}}, :start)
  end

  test "blocked queued work stays waiting without consuming a build timeout" do
    installation = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        repo_xref: 14,
        installation_id: installation.id
      })

    batch =
      Repo.insert!(%Batch{
        project_id: project.id,
        into_branch: "main",
        state: :waiting,
        last_polled: 0
      })

    before_wait = Repo.get!(Batch, batch.id)

    GitHub.ServerMock.put_state(%{
      {{:installation, 91}, 14} => %{backend_snapshot: {:ok, %{backend: "bors", github_count: 1}}}
    })

    assert {:noreply, id} = Batcher.handle_info({:poll, :once}, project.id)
    assert id == project.id
    after_wait = Repo.get!(Batch, batch.id)
    assert after_wait.state == :waiting
    assert after_wait.timeout_at == before_wait.timeout_at
    assert after_wait.commit == nil
  end

  test "pilot branches and other repositories are outside the handoff" do
    refute MergeBackend.scoped?(%Project{name: "TauCetiProject/TauCeti"}, "bors-pilot")
    refute MergeBackend.scoped?(%Project{name: "other/repo"}, "main")
    assert MergeBackend.allow(%Project{name: "other/repo"}, "main", :admit) == :ok
  end

  test "revoked and moved-head delayed approvals cannot activate" do
    installation = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        repo_xref: 14,
        installation_id: installation.id
      })

    patch =
      Repo.insert!(%BorsNG.Database.Patch{
        project_id: project.id,
        pr_xref: 1,
        into_branch: "main",
        commit: "old"
      })

    assert {:noreply, _} =
             Batcher.handle_info({:prerun_poll, 1, {:held_approval, patch}}, project.id)

    patch
    |> BorsNG.Database.Patch.changeset(%{commit: "new", bundle_reviewer: "reviewer"})
    |> Repo.update!()

    assert {:noreply, _} = Batcher.handle_info({:prerun_poll, 1, {"reviewer", patch}}, project.id)
    assert Repo.all(Batch.all_for_project(project.id)) == []
  end
end
