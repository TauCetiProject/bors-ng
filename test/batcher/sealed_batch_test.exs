defmodule BorsNG.Worker.SealedBatchTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{Batch, Installation, LinkPatchBatch, Patch, PatchBundle, Project, Repo}
  alias BorsNG.GitHub
  alias BorsNG.Worker.Batcher
  alias BorsNG.Worker.Batcher.Divider

  setup do
    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "project_name",
        installation_id: inst.id,
        repo_xref: 14,
        batch_delay_sec: 3600
      })

    GitHub.ServerMock.put_state(%{
      {{:installation, 91}, 14} => %{
        branches: %{},
        commits: %{},
        comments: Map.new(1..20, &{&1, []}),
        statuses: %{},
        files: %{}
      }
    })

    {:ok, project: project}
  end

  defp patch(project, pr, attrs \\ %{}) do
    Repo.insert!(
      struct!(
        Patch,
        Map.merge(
          %{project_id: project.id, pr_xref: pr, commit: "h#{pr}", into_branch: "master"},
          attrs
        )
      )
    )
  end

  defp source(project, patches) do
    batch = Repo.insert!(%Batch{Batch.new(project.id, "master") | state: :error})

    for p <- patches do
      Repo.insert!(%LinkPatchBatch{
        batch_id: batch.id,
        patch_id: p.id,
        reviewer: "reviewer",
        head_sha: p.commit,
        manual_approval: p.manual_approval
      })
    end

    Repo.all(LinkPatchBatch.from_batch(batch.id))
  end

  test "fresh arrivals skip a recently updated retry and fill a normal batch", %{project: project} do
    p = patch(project, 1, %{manual_approval: %{"head_sha" => "h1", "actor_id" => 42}})
    links = source(project, [p])
    retry = Divider.clone_batch(links, project.id, "master")
    assert retry.sealed
    Batcher.handle_cast({:reviewed, patch(project, 2).id, "r2"}, project.id)
    [normal] = Repo.all(from(b in Batch, where: b.state == :waiting and not b.sealed))

    Repo.update_all(from(b in Batch, where: b.id == ^retry.id),
      set: [updated_at: ~N[2099-01-01 00:00:00]]
    )

    Batcher.handle_cast({:reviewed, patch(project, 3).id, "r3"}, project.id)

    assert Enum.sort(Enum.map(Repo.all(LinkPatchBatch.from_batch(normal.id)), & &1.patch.pr_xref)) ==
             [2, 3]

    [retained] = Repo.all(LinkPatchBatch.from_batch(retry.id))
    assert retained.head_sha == "h1"
    assert retained.manual_approval == p.manual_approval
    assert retained.reviewer == "reviewer"
  end

  test "bisection seals both children and never admits an arrival to either", %{project: project} do
    links = source(project, for(pr <- 1..4, do: patch(project, pr)))
    parent = Repo.get!(Batch, hd(links).batch_id) |> Repo.preload(:project)
    assert Divider.split_batch(links, parent) == :retrying
    children = Repo.all(Batch.all_for_project(project.id, :waiting))
    assert length(children) == 2
    assert Enum.all?(children, & &1.sealed)
    Batcher.handle_cast({:reviewed, patch(project, 5).id, "new"}, project.id)
    [new_batch] = Repo.all(Batch.all_for_patch(Repo.get_by!(Patch, pr_xref: 5).id, :incomplete))
    refute new_batch.sealed
    refute new_batch.id in Enum.map(children, & &1.id)
    assert Enum.map(children, &length(Repo.all(LinkPatchBatch.from_batch(&1.id)))) == [2, 2]
  end

  test "a fresh bundle enters a normal batch intact beside sealed retry work", %{project: project} do
    retry = Divider.clone_batch(source(project, [patch(project, 1)]), project.id, "master")
    bundle = Repo.insert!(PatchBundle.new(project.id))
    p2 = patch(project, 2, %{bundle_id: bundle.id})
    p3 = patch(project, 3, %{bundle_id: bundle.id})
    Batcher.handle_cast({:reviewed, p2.id, "r2"}, project.id)
    Batcher.handle_cast({:reviewed, p3.id, "r3"}, project.id)
    [normal] = Repo.all(Batch.all_for_patch(p2.id, :incomplete))
    refute normal.sealed

    assert Enum.sort(Enum.map(Repo.all(LinkPatchBatch.from_batch(normal.id)), & &1.patch_id)) == [
             p2.id,
             p3.id
           ]

    assert length(Repo.all(LinkPatchBatch.from_batch(retry.id))) == 1
  end

  test "withdrawal from a retry does not reopen it to admissions", %{project: project} do
    p = patch(project, 1)
    sibling = patch(project, 3)
    retry = Divider.clone_batch(source(project, [p, sibling]), project.id, "master")
    Batcher.handle_cast({:cancel, p.id, :requested}, project.id)
    assert Enum.map(Repo.all(LinkPatchBatch.from_batch(retry.id)), & &1.patch_id) == [sibling.id]
    assert Repo.get!(Batch, retry.id).sealed
    Batcher.handle_cast({:reviewed, patch(project, 2).id, "r2"}, project.id)
    assert Enum.map(Repo.all(LinkPatchBatch.from_batch(retry.id)), & &1.patch_id) == [sibling.id]
  end

  test "sealing does not change priority selection or capacity limits", %{project: project} do
    retry =
      Divider.clone_batch(
        source(project, [patch(project, 1, %{priority: 2})]),
        project.id,
        "master"
      )

    assert retry.priority == 2
    {normal, true} = Batcher.get_new_batch(3, project.id, "master", 2, false, 2)
    refute normal.sealed

    for pr <- [2, 3] do
      p = patch(project, pr, %{priority: 2})

      Repo.insert!(%LinkPatchBatch{
        batch_id: normal.id,
        patch_id: p.id,
        reviewer: "r",
        head_sha: p.commit
      })
    end

    {same, false} = Batcher.get_new_batch(3, project.id, "master", 2, false)
    assert same.id == normal.id
    {other, true} = Batcher.get_new_batch(3, project.id, "master", 2, false, 2)
    assert other.id != normal.id
    {lower, true} = Batcher.get_new_batch(nil, project.id, "master", 1, false)
    assert lower.priority == 1
    {:waiting, ordered} = Batcher.sort_batches([lower, retry])
    assert hd(ordered).id == retry.id
  end

  test "restart and concurrent approval delivery retain the seal and one exact-head membership",
       %{project: project} do
    retry = Divider.clone_batch(source(project, [patch(project, 1)]), project.id, "master")
    {:ok, pid} = Batcher.start_link(project.id)
    assert :sys.get_state(pid) == project.id
    GenServer.stop(pid, :normal)
    {:ok, restarted} = Batcher.start_link(project.id)
    on_exit(fn -> if Process.alive?(restarted), do: GenServer.stop(restarted, :normal) end)
    members = for pr <- 2..10, do: patch(project, pr)

    members
    |> Enum.flat_map(fn p ->
      for _ <- 1..2, do: Task.async(fn -> Batcher.reviewed(restarted, p.id, "r") end)
    end)
    |> Enum.each(&Task.await/1)

    assert :sys.get_state(restarted) == project.id
    assert Repo.get!(Batch, retry.id).sealed
    assert length(Repo.all(LinkPatchBatch.from_batch(retry.id))) == 1
    [normal] = Repo.all(from(b in Batch, where: b.state == :waiting and not b.sealed))
    admitted = Repo.all(LinkPatchBatch.from_batch(normal.id))
    assert length(admitted) == 9

    assert Enum.sort(Enum.map(admitted, &{&1.patch_id, &1.head_sha})) ==
             Enum.sort(Enum.map(members, &{&1.id, &1.commit}))

    GenServer.stop(restarted, :normal)
  end
end
