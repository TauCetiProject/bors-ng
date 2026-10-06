defmodule BorsNG.QueueHandoffTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Batch, Installation, LinkPatchBatch, Patch, Project, Repo, Status}
  alias BorsNG.GitHub
  alias BorsNG.Worker.{Batcher, QueueHandoff}

  @conn {{:installation, 91}, 14}
  @selected_at "2026-10-06T00:08:34Z"

  defp tail(overrides \\ %{}) do
    %{
      "totalCount" => 39,
      "nodes" => [
        Map.merge(
          %{
            "id" => "entry-39",
            "position" => 39,
            "state" => "QUEUED",
            "headCommit" => nil,
            "pullRequest" => %{"id" => "pr-39", "number" => 39, "headRefOid" => "head"}
          },
          overrides
        )
      ]
    }
  end

  test "only an unbuilt last entry is removable, including queues larger than one page" do
    assert {:ok, %{pr: 39}} = QueueHandoff.queued_tail(tail())

    assert {:ok, _} =
             QueueHandoff.queued_tail(tail(%{"position" => 150}) |> Map.put("totalCount", 150))

    assert {:done, :empty} = QueueHandoff.queued_tail(%{"totalCount" => 0, "nodes" => []})

    for state <- ["AWAITING_CHECKS", "MERGEABLE", "LOCKED", "UNKNOWN"] do
      assert {:done, :already_started} = QueueHandoff.queued_tail(tail(%{"state" => state}))
    end

    assert {:done, :already_started} =
             QueueHandoff.queued_tail(tail(%{"headCommit" => %{"oid" => "staging"}}))

    assert {:error, _} = QueueHandoff.queued_tail(tail(%{"position" => 38}))
    assert {:error, _} = QueueHandoff.queued_tail(tail(%{"pullRequest" => nil}))
    assert {:error, _} = QueueHandoff.queued_tail(%{"totalCount" => 39, "nodes" => []})
    assert {:error, _} = QueueHandoff.queued_tail(nil)

    assert {:error, _} =
             QueueHandoff.queued_tail(
               tail()
               |> Map.update!("nodes", fn [n] -> [Map.delete(n, "headCommit")] end)
             )
  end

  test "trim is bounded and stops as soon as the live tail is active or a read fails" do
    snapshot = %{backend: "bors", updated_at: @selected_at, github_count: 39}
    results = Enum.map(39..20//-1, &{:ok, %{pr: &1, head_sha: "head"}})

    GitHub.ServerMock.put_state(%{
      @conn => %{backend_snapshot: {:ok, snapshot}, handoff_results: results}
    })

    assert Enum.map(QueueHandoff.trim(@conn, snapshot), & &1.pr) == Enum.to_list(39..24//-1)
    assert length(GitHub.ServerMock.get_state()[@conn].handoff_results) == 4

    for stop <- [{:done, :already_started}, {:error, :unauthorized}] do
      GitHub.ServerMock.put_state(%{
        @conn => %{
          backend_snapshot: {:ok, snapshot},
          handoff_results: [{:ok, %{pr: 39}}, stop, {:ok, %{pr: 37}}]
        }
      })

      assert QueueHandoff.trim(@conn, snapshot) == [%{pr: 39}]
      assert GitHub.ServerMock.get_state()[@conn].handoff_results == [{:ok, %{pr: 37}}]
    end
  end

  test "stale and superseded backend selections cannot transfer anything" do
    before = %{backend: "bors", updated_at: @selected_at, github_count: 39}

    for current <- [%{before | backend: "queue"}, %{before | updated_at: "later"}] do
      GitHub.ServerMock.put_state(%{
        @conn => %{backend_snapshot: {:ok, current}, handoff_results: [{:ok, %{pr: 39}}]}
      })

      assert QueueHandoff.trim(@conn, before) == []
      assert length(GitHub.ServerMock.get_state()[@conn].handoff_results) == 1
    end

    assert QueueHandoff.trim(@conn, %{before | backend: "queue"}) == []
  end

  test "heartbeat reports a fresh outgoing count after transferring waiting PRs" do
    project()
    snapshot = %{backend: "bors", updated_at: @selected_at, github_count: 3}

    GitHub.ServerMock.put_state(%{
      @conn => %{
        backend_snapshot: {:ok, snapshot},
        merge_candidates: {:ok, []},
        handoff_results: Enum.map(3..1//-1, &{:ok, %{pr: &1}})
      }
    })

    state = %{last_dispatch: nil, observation: nil}
    assert {:reply, observation, _} = BorsNG.Worker.MergeReconciler.handle_call(:tick, nil, state)
    assert observation.github_count == 0
    assert observation.released_github_prs == [3, 2, 1]
    assert observation.reason == "drained"
    refute observation.overlap
    :persistent_term.put({BorsNG.Worker.MergeReconciler, :observation}, nil)
  end

  defp project(name \\ "TauCetiProject/TauCeti") do
    inst = Repo.insert!(%Installation{installation_xref: 91})
    Repo.insert!(%Project{name: name, repo_xref: 14, installation_id: inst.id})
  end

  defp batch(project, state, commit, base \\ "main") do
    Repo.insert!(%Batch{
      project_id: project.id,
      into_branch: base,
      state: state,
      commit: commit,
      last_polled: 0
    })
  end

  defp patch_link(project, batch, pr, head \\ "head") do
    patch =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: pr,
        into_branch: batch.into_branch,
        open: true,
        commit: head,
        merge_eligibility_id: 100,
        merge_eligibility: %{"head_sha" => head}
      })

    Repo.insert!(%LinkPatchBatch{
      batch_id: batch.id,
      patch_id: patch.id,
      reviewer: "r",
      head_sha: head
    })

    patch
  end

  defp mock(snapshot) do
    GitHub.ServerMock.put_state(%{
      @conn => %{
        backend_snapshot: snapshot,
        files: %{"main" => %{"bors.toml" => "status = [\"build\"]\n"}},
        branches: %{}
      }
    })
  end

  test "bors transfers only unstarted main batches and preserves exact-head approvals" do
    project = project()
    waiting = batch(project, :waiting, nil)
    patch = patch_link(project, waiting, 1)
    running = batch(project, :running, "staging")
    held_build = batch(project, :waiting, "old-staging")
    Repo.update!(Batch.changeset(held_build, %{timeout_at: 42}))
    pilot = batch(project, :waiting, nil, "bors-pilot")
    mock({:ok, %{backend: "queue", github_count: 0}})
    Batcher.release_unstarted_batches(project)
    assert Repo.get!(Batch, waiting.id).state == :canceled
    assert Repo.get!(Batch, running.id).state == :running
    assert Repo.get!(Batch, held_build.id).state == :canceled
    assert Repo.get!(Batch, pilot.id).state == :waiting
    preserved = Repo.get!(Patch, patch.id)
    assert preserved.bundle_reviewer == "r"
    assert preserved.merge_eligibility_id == 100
    assert preserved.merge_eligibility == patch.merge_eligibility
    assert Repo.one(LinkPatchBatch.from_batch(waiting.id)).head_sha == "head"
    assert Process.get({:approval_pending, patch.id}) == nil

    # A restart/repeated tick sees archived work, not another transfer.
    Batcher.release_unstarted_batches(project)
    assert Repo.get!(Patch, patch.id).bundle_reviewer == "r"
    Batcher.do_handle_cast({:cancel, patch.id, :requested}, project.id)
    assert Repo.get!(Patch, patch.id).bundle_reviewer == nil
  end

  test "moved-head, closed and drafted patches gain no held approval from a transfer" do
    project = project()
    waiting = batch(project, :waiting, nil)
    moved = patch_link(project, waiting, 1)
    closed = patch_link(project, waiting, 2)
    draft = patch_link(project, waiting, 3)
    Repo.update!(Patch.changeset(moved, %{commit: "new"}))
    Repo.update!(Patch.changeset(closed, %{open: false}))
    Repo.update!(Patch.changeset(draft, %{is_draft: true}))
    mock({:ok, %{backend: "queue", github_count: 0}})
    Batcher.release_unstarted_batches(project)
    assert Repo.get!(Batch, waiting.id).state == :canceled

    for patch <- [moved, closed, draft],
        do: assert(Repo.get!(Patch, patch.id).bundle_reviewer == nil)
  end

  test "failed observations and bors selection keep waiting work intact" do
    project = project()
    waiting = batch(project, :waiting, nil)

    for snapshot <- [
          {:error, :unauthorized},
          {:ok, %{backend: "bors", github_count: 0}},
          {:ok, %{backend: "invalid", github_count: 0}}
        ] do
      mock(snapshot)
      Batcher.release_unstarted_batches(project)
      assert Repo.get!(Batch, waiting.id).state == :waiting
    end
  end

  test "restart between CI dispatch and running-state persistence cannot transfer the build" do
    project = project()
    armed = batch(project, :waiting, nil)
    orphaned_status = batch(project, :waiting, nil)
    Repo.update!(Batch.changeset(armed, %{timeout_at: 42}))
    Repo.insert!(%Status{batch_id: orphaned_status.id, identifier: "build", state: :running})
    mock({:ok, %{backend: "queue", github_count: 0}})
    Batcher.release_unstarted_batches(project)
    assert Repo.get!(Batch, armed.id).state == :waiting
    assert Repo.get!(Batch, orphaned_status.id).state == :waiting
  end

  test "other repositories do not transfer" do
    project = project("other/repo")
    waiting = batch(project, :waiting, nil)
    mock({:ok, %{backend: "queue", github_count: 0}})
    Batcher.release_unstarted_batches(project)
    assert Repo.get!(Batch, waiting.id).state == :waiting
  end

  test "dormant transferred approvals do not poll preflight under GitHub selection" do
    project = project()
    waiting = batch(project, :waiting, nil)
    patch = patch_link(project, waiting, 1)
    mock({:ok, %{backend: "queue", github_count: 0}})
    Batcher.release_unstarted_batches(project)
    # No pulls/eligibility/status/review fixtures exist: any preflight would fail.
    assert {:noreply, _} = Batcher.handle_info(:wake_backend_holds, project.id)
    assert {:noreply, _} = Batcher.handle_info(:recover_backend_holds, project.id)
    assert Repo.get!(Patch, patch.id).bundle_reviewer == "r"
    assert Process.get({:approval_pending, patch.id}) == nil
    assert Repo.all(Batch.all_for_project(project.id, :incomplete)) == []
  end

  test "expired trim budget does not enqueue a mutation" do
    snapshot = %{backend: "bors", updated_at: @selected_at, github_count: 39}

    GitHub.ServerMock.put_state(%{
      @conn => %{backend_snapshot: {:ok, snapshot}, handoff_results: [{:ok, %{pr: 39}}]}
    })

    assert QueueHandoff.trim(@conn, snapshot, System.monotonic_time(:millisecond) - 1) == []
    assert length(GitHub.ServerMock.get_state()[@conn].handoff_results) == 1
  end

  test "deleted projects keep the existing clean-stop behavior" do
    assert {:stop, :normal, -1} = Batcher.handle_info({:poll, :once}, -1)
  end
end
