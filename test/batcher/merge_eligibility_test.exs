defmodule BorsNG.Worker.MergeEligibilityTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Batch, Installation, LinkPatchBatch, Patch, Project, Repo, User, Status}
  alias BorsNG.GitHub
  alias BorsNG.Worker.{Batcher, ManualApproval, MergeEligibility, QueueApproval, Syncer}

  @head String.duplicate("a", 40)
  @base String.duplicate("b", 40)
  @conn {{:installation, 91}, 14}

  setup do
    old = System.get_env("TAUCETI_REVIEW_APP_ID")
    System.put_env("TAUCETI_REVIEW_APP_ID", "3947238")

    on_exit(fn ->
      if old,
        do: System.put_env("TAUCETI_REVIEW_APP_ID", old),
        else: System.delete_env("TAUCETI_REVIEW_APP_ID")
    end)

    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        installation_id: inst.id,
        repo_xref: 14
      })

    pr = %GitHub.Pr{
      number: 1,
      state: :open,
      base_ref: "main",
      head_sha: @head,
      user: %GitHub.User{id: 6, login: "author", avatar_url: "avatar"}
    }

    toml = "status = [\"build\"]\npr_status = [\"build\"]\n"

    GitHub.ServerMock.put_state(%{
      @conn => %{
        pulls: %{1 => pr},
        labels: %{1 => []},
        comments: %{1 => []},
        statuses: %{@head => %{"build" => :ok}},
        branches: %{},
        files: %{@head => %{"bors.toml" => toml}, "main" => %{"bors.toml" => toml}},
        eligibility_checks: %{@head => {:ok, [check(10)]}},
        merge_bases: %{{1, @head} => {:ok, @base}},
        backend_snapshot: {:ok, %{backend: "bors", github_count: 0}}
      }
    })

    {:ok, project: project}
  end

  defp check(id, overrides \\ %{}) do
    d = %{
      "schema" => "tauceti-merge.eligibility/v1",
      "repo" => "TauCetiProject/TauCeti",
      "pr" => 1,
      "eligible" => true,
      "review_safe" => true,
      "single" => false,
      "merge_base_sha" => @base
    }

    %{
      "id" => id,
      "name" => "merge eligibility",
      "app" => %{"id" => 3_947_238},
      "status" => "completed",
      "conclusion" => "success",
      "head_sha" => @head,
      "external_id" => Jason.encode!(Map.merge(d, overrides))
    }
    |> Map.put(
      "conclusion",
      if(Map.get(overrides, "review_safe") == false,
        do: "failure",
        else: if(Map.get(overrides, "eligible") == false, do: "neutral", else: "success")
      )
    )
  end

  defp update_repo(key, value) do
    GitHub.ServerMock.get_state() |> put_in([@conn, key], value) |> GitHub.ServerMock.put_state()
  end

  defp failed_bump(project, legacy \\ false) do
    pr = GitHub.ServerMock.get_state()[@conn].pulls[1]

    pr = %{
      pr
      | head_ref: "bump-mathlib/fix-test",
        head_repo_id: 14,
        base_repo_id: 14,
        user: %{pr.user | login: "tauceti-review-bot[bot]"}
    }

    update_repo(:pulls, %{1 => pr})
    update_repo(:eligibility_checks, %{@head => {:ok, [check(10, %{"single" => true})]}})
    MergeEligibility.reconcile(project, 1)
    patch = Repo.get_by!(Patch, project_id: project.id, pr_xref: 1)
    batch = Repo.one!(Batch.all_for_project(project.id, :incomplete))
    staged = String.duplicate("d", 40)

    batch =
      batch
      |> Batch.changeset(%{
        state: :error,
        commit: staged,
        base_sha: if(legacy, do: nil, else: @base)
      })
      |> Repo.update!()

    QueueApproval.fail([patch], batch, :build)

    if legacy do
      patch |> Patch.changeset(%{queue_approval: nil}) |> Repo.update!()
      update_repo(:commits, %{staged => %{parents: [@base]}})
    end

    current = String.duplicate("c", 40)
    update_repo(:branches, %{"main" => current})
    update_repo(:compare_status, %{{@base, current} => :ahead})
    {Repo.get!(Patch, patch.id), batch, current}
  end

  test "advanced main retries a failed singleton with the same check and head", %{project: p} do
    {patch, _batch, _current} = failed_bump(p)
    MergeEligibility.reconcile(p, 1)
    updated = Repo.get!(Patch, patch.id)
    assert updated.commit == @head
    assert updated.merge_eligibility_id == 10
    assert updated.merge_eligibility["bump_retry_count"] == 1
    assert updated.is_single
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 2
    assert Repo.aggregate(Batch.all_for_project(p.id, :incomplete), :count, :id) == 1
    MergeEligibility.reconcile(p, 1)
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 2
    assert GitHub.ServerMock.get_state()[@conn].pulls[1].head_sha == @head
  end

  test "legacy singleton base is read from its staging commit, without changing the branch", %{
    project: p
  } do
    {patch, _, _} = failed_bump(p, true)
    MergeEligibility.reconcile(p, 1)
    assert Repo.get!(Patch, patch.id).merge_eligibility["bump_retry_count"] == 1
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 2
  end

  test "withdrawal and pending intent remain stopped even after main advances", %{project: p} do
    {patch, _, _} = failed_bump(p)

    for reason <- [:requested, :cancel_all, :automatic_review_rejected] do
      QueueApproval.stop([Repo.get!(Patch, patch.id)], reason)
      MergeEligibility.reconcile(p, 1)
      assert Repo.aggregate(LinkPatchBatch, :count, :id) == 1
      refute Repo.get!(Patch, patch.id).queue_pending
    end

    patch
    |> Patch.changeset(%{
      queue_pending: true,
      queue_approval: %{"state" => "approved", "head_sha" => @head, "reviewer" => "r"}
    })
    |> Repo.update!()

    MergeEligibility.reconcile(p, 1)
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 1
  end

  test "holds and non-forward main defer without charging recovery", %{project: p} do
    {patch, _, current} = failed_bump(p)
    update_repo(:labels, %{1 => ["hold"]})
    MergeEligibility.reconcile(p, 1)
    assert Repo.get!(Patch, patch.id).merge_eligibility["bump_retry_count"] == nil
    update_repo(:labels, %{1 => []})
    update_repo(:compare_status, %{{@base, current} => :diverged})
    MergeEligibility.reconcile(p, 1)
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 1
  end

  test "recovery is capped across main changes and hands genuine failure to bump repair", %{
    project: p
  } do
    {patch, old, first} = failed_bump(p)

    Enum.reduce(1..3, {old, first}, fn count, {_failed, current} ->
      MergeEligibility.reconcile(p, 1)
      patch = Repo.get!(Patch, patch.id)
      assert patch.merge_eligibility["bump_retry_count"] == count
      batch = Repo.one!(Batch.all_for_project(p.id, :incomplete))
      batch = batch |> Batch.changeset(%{state: :error, base_sha: current}) |> Repo.update!()
      QueueApproval.fail([patch], batch, :build)
      next = String.duplicate(Integer.to_string(count + 4), 40)
      update_repo(:branches, %{"main" => next})
      update_repo(:compare_status, %{{current, next} => :ahead})
      {batch, next}
    end)

    MergeEligibility.reconcile(p, 1)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    assert Repo.get!(Patch, patch.id).merge_eligibility["bump_retry_count"] == 3
    assert GitHub.ServerMock.get_state()[@conn].statuses[@head]["bump-integration"] == :error

    assert [%{pr: 1, head_sha: @head}] =
             Enum.map(
               BorsNG.Worker.BumpRecovery.repair_candidates(p.id, "main"),
               &Map.take(&1, [:pr, :head_sha])
             )
  end

  test "unchanged main and unavailable ancestry do not spend a retry", %{project: p} do
    {patch, _, current} = failed_bump(p)
    update_repo(:branches, %{"main" => @base})
    MergeEligibility.reconcile(p, 1)
    assert Repo.get!(Patch, patch.id).merge_eligibility["bump_retry_count"] == nil
    update_repo(:branches, %{"main" => current})
    update_repo(:compare_status, %{})
    MergeEligibility.reconcile(p, 1)
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 1
  end

  test "new check IDs preserve the same-head retry budget; a new head resets it", %{project: p} do
    {patch, _, _} = failed_bump(p)
    MergeEligibility.reconcile(p, 1)
    patch = Repo.get!(Patch, patch.id)
    decision = patch.merge_eligibility |> Map.put("id", 11) |> Map.delete("bump_retry_count")
    assert BorsNG.Worker.BumpRecovery.record(decision, patch, nil)["bump_retry_count"] == 1
    changed = %{patch | commit: String.duplicate("e", 40)}
    assert BorsNG.Worker.BumpRecovery.record(decision, changed, nil)["bump_retry_count"] == nil
  end

  test "repair handoff excludes pending, withdrawn, held and active intent", %{project: p} do
    {patch, batch, _} = failed_bump(p)
    candidates = fn -> BorsNG.Worker.BumpRecovery.repair_candidates(p.id, "main") end
    assert [%{pr: 1, batch_id: id}] = candidates.()
    assert id == batch.id

    for changes <- [
          %{queue_pending: true},
          %{bundle_reviewer: "reviewer"},
          %{commit: String.duplicate("f", 40)}
        ] do
      patch |> Patch.changeset(changes) |> Repo.update!()
      assert candidates.() == []

      Repo.update_all(from(x in Patch, where: x.id == ^patch.id),
        set: [queue_pending: false, bundle_reviewer: nil, commit: @head]
      )
    end

    # Restore every field explicitly; updating an unchanged stale struct is not a reset.
    Repo.update_all(from(x in Patch, where: x.id == ^patch.id),
      set: [queue_pending: false, bundle_reviewer: nil, commit: @head]
    )

    active = Repo.insert!(%Batch{project_id: p.id, into_branch: "main", state: :waiting})

    Repo.insert!(%LinkPatchBatch{
      batch_id: active.id,
      patch_id: patch.id,
      head_sha: @head,
      reviewer: "r"
    })

    assert candidates.() == []
    active |> Batch.changeset(%{state: :canceled}) |> Repo.update!()
    QueueApproval.stop([Repo.get!(Patch, patch.id)], :requested)
    assert candidates.() == []
  end

  test "known bump is offered at most once per five minutes without candidate discovery", %{
    project: p
  } do
    {_, _, _} = failed_bump(p)
    {bump, state} = BorsNG.Worker.MergeReconciler.offer_bump(p, %{}, 1000)
    assert bump.pr == 1
    assert {nil, ^state} = BorsNG.Worker.MergeReconciler.offer_bump(p, state, 1299)

    assert {^bump, %{last_bump_offer: 1300}} =
             BorsNG.Worker.MergeReconciler.offer_bump(p, state, 1300)

    # Drain the asynchronous eligibility casts before releasing the test sandbox.
    :sys.get_state(Batcher.Registry.get(p.id))
  end

  test "transferred automatic approval survives restart and a switch back without a new check", %{
    project: project
  } do
    MergeEligibility.reconcile(project, 1)
    patch = Repo.get_by!(Patch, project_id: project.id, pr_xref: 1)
    original = Repo.one!(Batch.all_for_project(project.id, :incomplete))
    assert original.state == :waiting
    update_repo(:backend_snapshot, {:ok, %{backend: "queue", github_count: 0}})
    Batcher.release_unstarted_batches(project)
    assert Repo.get!(Batch, original.id).state == :canceled
    held = Repo.get!(Patch, patch.id)
    assert held.bundle_reviewer == "tauceti-review-bot[bot]"
    assert held.merge_eligibility_id == patch.merge_eligibility_id
    assert held.merge_eligibility == patch.merge_eligibility

    # Simulate a fresh batcher with no in-memory generation or recovery flag.
    Process.delete({:approval_pending, patch.id})
    Process.delete(:backend_holds_recovered)
    update_repo(:backend_snapshot, {:ok, %{backend: "bors", github_count: 0}})
    assert {:noreply, _} = Batcher.handle_info(:recover_backend_holds, project.id)
    recovered = Repo.one!(Batch.all_for_project(project.id, :incomplete))
    assert recovered.id != original.id
    assert recovered.state == :waiting
    assert Repo.one!(LinkPatchBatch.from_batch(recovered.id)).head_sha == @head
    assert Repo.get!(Patch, patch.id).merge_eligibility_id == patch.merge_eligibility_id

    # Newly transferred holds must also wake after startup recovery already ran.
    update_repo(:backend_snapshot, {:ok, %{backend: "queue", github_count: 0}})
    Batcher.release_unstarted_batches(project)
    update_repo(:backend_snapshot, {:ok, %{backend: "bors", github_count: 0}})
    assert {:noreply, _} = Batcher.handle_info(:wake_backend_holds, project.id)
    reawakened = Repo.one!(Batch.all_for_project(project.id, :incomplete))
    assert reawakened.id != recovered.id
    assert Repo.get!(Patch, patch.id).merge_eligibility_id == patch.merge_eligibility_id
  end

  test "newest authentic check wins; replay, forged App and wrong head cannot approve" do
    green = check(10)
    revoke = check(11, %{"eligible" => false, "review_safe" => false})
    assert {:ok, %{"review_safe" => false}} = MergeEligibility.latest([revoke, green], 1, @head)
    assert {:ok, nil} = MergeEligibility.latest([Map.put(green, "app", %{"id" => 99})], 1, @head)
    assert {:ok, nil} = MergeEligibility.latest([green], 1, String.duplicate("c", 40))
    assert {:ok, nil} = MergeEligibility.latest([green], 2, @head)
    malformed = green |> Map.put("id", 15) |> Map.put("external_id", "{bad")

    assert {:error, :invalid_eligibility_check} =
             MergeEligibility.latest([green, malformed], 1, @head)

    assert {:error, :invalid_eligibility_check} =
             MergeEligibility.latest([Map.put(green, "conclusion", "failure")], 1, @head)

    assert {:error, :invalid_eligibility_check} =
             MergeEligibility.latest([check(12, %{"merge_base_sha" => nil})], 1, @head)
  end

  test "automatically admits once without a command and preserves single mode", %{project: p} do
    update_repo(:eligibility_checks, %{@head => {:ok, [check(10, %{"single" => true})]}})
    MergeEligibility.reconcile(p, 1)
    patch = Repo.get_by!(Patch, project_id: p.id, pr_xref: 1)
    assert patch.merge_eligibility_id == 10
    assert patch.is_single
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 1
    MergeEligibility.reconcile(p, 1)
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 1
    Batcher.do_handle_cast({:cancel, patch.id, :requested}, p.id)
    MergeEligibility.reconcile(p, 1)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end

  test "a consumed eligibility check recovers a missing membership without a new approval", %{
    project: p
  } do
    MergeEligibility.reconcile(p, 1)
    patch = Repo.get_by!(Patch, project_id: p.id, pr_xref: 1)
    old = Repo.one!(Batch.all_for_project(p.id, :incomplete))
    assert patch.queue_pending
    assert patch.queue_approval["head_sha"] == @head
    Repo.delete!(old)
    Process.delete(:queue_recovery_cursor)

    Batcher.handle_info(:recover_queue, p.id)
    recovered = Repo.one!(Batch.all_for_project(p.id, :incomplete))
    assert recovered.id != old.id
    assert Repo.one!(LinkPatchBatch.from_batch(recovered.id)).head_sha == @head
    assert Repo.get!(Patch, patch.id).merge_eligibility_id == 10
    Batcher.handle_info(:recover_queue, p.id)
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 1
  end

  test "recovery does not restart an existing approval waiting for CI", %{project: p} do
    update_repo(:statuses, %{@head => %{"build" => :running}})
    MergeEligibility.reconcile(p, 1)
    patch = Repo.get_by!(Patch, project_id: p.id, pr_xref: 1)
    before = GitHub.ServerMock.get_state()
    assert patch.bundle_reviewer
    for _ <- 1..3, do: Batcher.handle_info(:recover_queue, p.id)
    assert GitHub.ServerMock.get_state() == before
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end

  test "recovery pins the accepted head even if the stored head moves before admission", %{
    project: p
  } do
    patch = Syncer.sync_patch(p.id, get_in(GitHub.ServerMock.get_state(), [@conn, :pulls, 1]))
    Batcher.do_handle_cast({:recover_reviewed, patch.id, @base, "reviewer"}, p.id)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    refute Repo.get!(Patch, patch.id).queue_pending
  end

  test "explicit withdrawal survives missing history and a newer eligibility check", %{project: p} do
    MergeEligibility.reconcile(p, 1)
    patch = Repo.get_by!(Patch, project_id: p.id, pr_xref: 1)
    Batcher.do_handle_cast({:cancel, patch.id, :requested}, p.id)
    update_repo(:eligibility_checks, %{@head => {:ok, [check(11)]}})
    MergeEligibility.reconcile(p, 1)
    Batcher.handle_info(:recover_queue, p.id)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    refute Repo.get!(Patch, patch.id).queue_pending
  end

  test "orphan recovery honors live holds, API failures and changed heads", %{project: p} do
    MergeEligibility.reconcile(p, 1)
    batch = Repo.one!(Batch.all_for_project(p.id, :incomplete))
    Repo.delete!(batch)
    update_repo(:labels, %{1 => ["Hold"]})
    Batcher.handle_info(:recover_queue, p.id)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    update_repo(:labels, %{1 => []})
    GitHub.ServerMock.put_state(Map.put(GitHub.ServerMock.get_state(), :get_pr_error, 1))
    Batcher.handle_info(:recover_queue, p.id)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    pr = get_in(GitHub.ServerMock.get_state(), [@conn, :pulls, 1])
    update_repo(:pulls, %{1 => %{pr | head_sha: String.duplicate("c", 40)}})
    Batcher.handle_info(:recover_queue, p.id)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end

  test "confirmed failure remains stopped until an explicit fresh review", %{project: p} do
    MergeEligibility.reconcile(p, 1)
    patch = Repo.get_by!(Patch, project_id: p.id, pr_xref: 1)
    batch = Repo.one!(Batch.all_for_project(p.id, :incomplete))
    QueueApproval.fail([patch], batch, :build)
    batch |> Batch.changeset(%{state: :error}) |> Repo.update!()
    update_repo(:eligibility_checks, %{@head => {:ok, [check(11)]}})
    MergeEligibility.reconcile(p, 1)
    Batcher.handle_info(:recover_queue, p.id)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    Batcher.do_handle_cast({:reviewed, patch.id, "human"}, p.id)
    assert Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end

  test "queue selection, drainage and failed observations defer automatic admission", %{
    project: p
  } do
    for snapshot <- [
          {:ok, %{backend: "queue", github_count: 0}},
          {:ok, %{backend: "bors", github_count: 1}},
          {:error, :unavailable}
        ] do
      update_repo(:backend_snapshot, snapshot)
      MergeEligibility.reconcile(p, 1)
      patch = Repo.get_by!(Patch, project_id: p.id, pr_xref: 1)
      assert is_nil(patch.merge_eligibility_id)
      refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    end
  end

  test "neutral preserves approval; unsafe review withdraws even with native queue selected", %{
    project: p
  } do
    MergeEligibility.reconcile(p, 1)
    update_repo(:eligibility_checks, %{@head => {:ok, [check(11, %{"eligible" => false})]}})
    MergeEligibility.reconcile(p, 1)
    assert Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    update_repo(:backend_snapshot, {:ok, %{backend: "queue", github_count: 0}})

    update_repo(:eligibility_checks, %{
      @head => {:ok, [check(12, %{"eligible" => false, "review_safe" => false})]}
    })

    MergeEligibility.reconcile(p, 1)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    patch = Repo.get_by!(Patch, project_id: p.id, pr_xref: 1)
    assert patch.merge_eligibility_id == 12
    assert is_nil(patch.bundle_reviewer)
  end

  test "changed merge base and human pause labels prevent admission", %{project: p} do
    update_repo(:merge_bases, %{{1, @head} => {:ok, String.duplicate("c", 40)}})
    MergeEligibility.reconcile(p, 1)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    update_repo(:merge_bases, %{{1, @head} => {:ok, @base}})
    update_repo(:labels, %{1 => ["Human"]})
    MergeEligibility.reconcile(p, 1)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end

  test "a missed push cancels old admission before the new head has any check", %{project: p} do
    MergeEligibility.reconcile(p, 1)
    state = GitHub.ServerMock.get_state()
    pr = get_in(state, [@conn, :pulls, 1])
    update_repo(:pulls, %{1 => %{pr | head_sha: String.duplicate("c", 40)}})
    MergeEligibility.reconcile(p, 1)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end

  test "terminal failures do not automatically retry", %{project: p} do
    MergeEligibility.reconcile(p, 1)
    batch = Repo.one!(Batch.all_for_project(p.id, :incomplete))
    batch |> Batch.changeset(%{state: :error}) |> Repo.update!()
    update_repo(:eligibility_checks, %{@head => {:ok, [check(11)]}})
    MergeEligibility.reconcile(p, 1)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    assert Repo.aggregate(LinkPatchBatch, :count, :id) == 1
  end

  test "preflight repairs missed revocation and checks the reviewed diff", %{project: p} do
    MergeEligibility.reconcile(p, 1)
    patch = Repo.get_by!(Patch, project_id: p.id, pr_xref: 1)
    assert :ok = MergeEligibility.preflight(@conn, patch)
    update_repo(:eligibility_checks, %{@head => {:ok, [check(11, %{"eligible" => false})]}})
    assert :ok = MergeEligibility.preflight(@conn, patch)
    update_repo(:merge_bases, %{{1, @head} => {:ok, String.duplicate("c", 40)}})
    assert {:error, :review_unsafe} = MergeEligibility.preflight(@conn, patch)
    update_repo(:eligibility_checks, %{@head => {:error, :unavailable}})
    assert :waiting = MergeEligibility.preflight(@conn, patch)
  end

  defp manual_review(project) do
    patch =
      Syncer.sync_patch(project.id, get_in(GitHub.ServerMock.get_state(), [@conn, :pulls, 1]))

    Batcher.do_handle_cast(
      {:maintainer_reviewed, patch.id, @head, 477_956, "kim-em", DateTime.utc_now()},
      project.id
    )

    Repo.get!(Patch, patch.id)
  end

  defp scope_ci() do
    toml =
      "status = [\"build\", \"scope\", \"bump-guard\"]\npr_status = [\"build\", \"scope\", \"bump-guard\"]\n"

    update_repo(:files, %{@head => %{"bors.toml" => toml}, "main" => %{"bors.toml" => toml}})
    update_repo(:statuses, %{@head => %{"build" => :ok, "bump-guard" => :ok, "scope" => :error}})
  end

  test "scope authority follows the actual GitHub user ID, not a claimed reviewer name", %{
    project: p
  } do
    assert ManualApproval.authorized?(p, %User{user_xref: 477_956, login: "renamed"})
    refute ManualApproval.authorized?(p, %User{user_xref: 1, login: "kim-em"})
    refute ManualApproval.authorized?(%{p | name: "fork/TauCeti"}, %User{user_xref: 477_956})
    patch = Syncer.sync_patch(p.id, get_in(GitHub.ServerMock.get_state(), [@conn, :pulls, 1]))
    assert {:error, _} = ManualApproval.grant(@conn, p, patch, 1, @head)
    assert {:error, _} = ManualApproval.grant(@conn, p, patch, 477_956, @base)
    assert is_nil(Repo.get!(Patch, patch.id).manual_approval)
  end

  test "human approval requests CI and waits for real scope success", %{project: p} do
    scope_ci()
    patch = manual_review(p)
    assert ManualApproval.valid?(patch)
    assert patch.manual_approval["merge_base_sha"] == @base
    assert patch.bundle_reviewer == "kim-em"
    assert get_in(GitHub.ServerMock.get_state(), [@conn, :pr_build_dispatches]) == [1]
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    update_repo(:statuses, %{@head => %{"build" => :ok, "bump-guard" => :ok, "scope" => :ok}})
    Batcher.do_handle_cast({:reviewed, patch.id, "kim-em"}, p.id)
    batch = Repo.one!(Batch.all_for_project(p.id, :incomplete))
    link = Repo.one!(LinkPatchBatch.from_batch(batch.id))
    assert link.manual_approval == patch.manual_approval
    clone = Batcher.Divider.clone_batch([link], p.id, "main")
    assert Repo.one!(LinkPatchBatch.from_batch(clone.id)).manual_approval == patch.manual_approval
  end

  test "manual approval supersedes an automatic verdict but never a failed build", %{project: p} do
    # Start with an automatic approval, then replace it with the human decision.
    MergeEligibility.reconcile(p, 1)
    patch = manual_review(p)
    assert is_nil(patch.merge_eligibility)
    batch = Repo.one!(Batch.all_for_project(p.id, :incomplete))
    assert Repo.one!(LinkPatchBatch.from_batch(batch.id)).manual_approval == patch.manual_approval

    update_repo(:eligibility_checks, %{
      @head => {:ok, [check(11, %{"eligible" => false, "review_safe" => false})]}
    })

    MergeEligibility.reconcile(p, 1)
    assert Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    Batcher.do_handle_cast({:cancel, patch.id, :requested}, p.id)
    assert is_nil(Repo.get!(Patch, patch.id).manual_approval)
    assert get_in(GitHub.ServerMock.get_state(), [@conn, :statuses, @head, "scope"]) == :running
    scope_ci()

    update_repo(:statuses, %{
      @head => %{"build" => :error, "bump-guard" => :ok, "scope" => :error}
    })

    manual_review(p)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end

  test "changed merge base cancels a manual approval even under the same head", %{project: p} do
    patch = manual_review(p)
    assert Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    update_repo(:merge_bases, %{{1, @head} => {:ok, String.duplicate("c", 40)}})
    assert {:error, :manual_approval_changed} = ManualApproval.preflight(@conn, patch)
    MergeEligibility.reconcile(p, 1)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    assert is_nil(Repo.get!(Patch, patch.id).manual_approval)
  end

  test "push, retarget, draft, and close invalidate stored human approval", %{project: p} do
    for changes <- [%{head_sha: @base}, %{base_ref: "pilot"}, %{draft: true}, %{state: :closed}] do
      pr = get_in(GitHub.ServerMock.get_state(), [@conn, :pulls, 1])
      patch = Syncer.sync_patch(p.id, pr)
      assert {:ok, _} = ManualApproval.grant(@conn, p, patch, 477_956, @head)
      synced = Syncer.sync_patch(p.id, struct(pr, changes))
      assert is_nil(synced.manual_approval)
      Syncer.sync_patch(p.id, pr)
    end
  end

  test "failed observations cannot grant human approval or admit stale diffs", %{project: p} do
    patch = Syncer.sync_patch(p.id, get_in(GitHub.ServerMock.get_state(), [@conn, :pulls, 1]))
    update_repo(:merge_bases, %{{1, @head} => {:error, :unavailable}})
    assert :waiting = ManualApproval.grant(@conn, p, patch, 477_956, @head)
    assert is_nil(Repo.get!(Patch, patch.id).manual_approval)
    update_repo(:merge_bases, %{{1, @head} => {:ok, @base}})
    assert {:ok, patch} = ManualApproval.grant(@conn, p, patch, 477_956, @head)
    update_repo(:merge_bases, %{{1, @head} => {:error, :unavailable}})
    assert :waiting = ManualApproval.preflight(@conn, patch)
  end

  test "the final push rechecks human approval after a green staging result", %{project: p} do
    patch = manual_review(p)
    batch = Repo.one!(Batch.all_for_project(p.id, :incomplete))
    staged = String.duplicate("d", 40)
    batch |> Batch.changeset(%{state: :running, commit: staged}) |> Repo.update!()
    Repo.insert!(%Status{batch_id: batch.id, identifier: "build", state: :running})
    update_repo(:merge_bases, %{{1, @head} => {:ok, @base}})
    # A failed live read holds the built batch instead of pushing it.
    update_repo(:merge_bases, %{{1, @head} => {:error, :unavailable}})
    Repo.update_all(from(b in Batch, where: b.id == ^batch.id), set: [promotion_retry_at: 0])
    Batcher.do_handle_cast({:status, {staged, "build", :ok, "https://ci"}}, p.id)
    assert Repo.get!(Batch, batch.id).state == :running
    update_repo(:merge_bases, %{{1, @head} => {:ok, String.duplicate("c", 40)}})
    Repo.update_all(from(b in Batch, where: b.id == ^batch.id), set: [promotion_retry_at: 0])
    Batcher.do_handle_cast({:status, {staged, "build", :ok, "https://ci"}}, p.id)
    assert Repo.get!(Batch, batch.id).state == :canceled
    assert is_nil(Repo.get!(Patch, patch.id).manual_approval)
  end

  test "delayed comments cannot approve a revision observed after they were written", %{
    project: p
  } do
    patch = Syncer.sync_patch(p.id, get_in(GitHub.ServerMock.get_state(), [@conn, :pulls, 1]))
    old = DateTime.add(patch.manual_revision_at, -60, :second) |> DateTime.to_iso8601()

    assert {:error, :manual_approval_changed} =
             ManualApproval.grant(@conn, p, patch, 477_956, @head, old)

    assert {:error, :manual_approval_changed} =
             ManualApproval.grant(@conn, p, patch, 477_956, @head, nil)
  end

  test "legacy automatic cancellation preserves human approval", %{project: p} do
    patch = manual_review(p)
    Batcher.do_handle_cast({:cancel, patch.id, :review_unsafe}, p.id)
    assert ManualApproval.valid?(Repo.get!(Patch, patch.id))
    Batcher.do_handle_cast({:cancel, patch.id, :requested}, p.id)
    refute ManualApproval.valid?(Repo.get!(Patch, patch.id))
  end

  test "a retry by another member cannot extend kim-em's scope authority after a push", %{
    project: p
  } do
    scope_ci()
    patch = manual_review(p)
    kim = Repo.insert!(%User{user_xref: 477_956, login: "kim-em"})
    BorsNG.Database.Context.Logging.log_cmd(patch, kim, :activate)
    state = GitHub.ServerMock.get_state()
    pr = get_in(state, [@conn, :pulls, 1])
    newer = %{pr | head_sha: @base}
    update_repo(:pulls, %{1 => newer})
    toml = get_in(state, [@conn, :files, "main", "bors.toml"])
    update_repo(:files, %{@base => %{"bors.toml" => toml}, "main" => %{"bors.toml" => toml}})
    update_repo(:statuses, %{@base => %{"build" => :ok, "bump-guard" => :ok, "scope" => :error}})
    update_repo(:merge_bases, %{{1, @base} => {:ok, @head}})
    patch = Syncer.sync_patch(p.id, newer)
    caller = %User{user_xref: 6, login: "author"}

    BorsNG.Command.run(
      %BorsNG.Command{
        project: p,
        patch: patch,
        commenter: caller,
        approval_created_at: DateTime.to_iso8601(DateTime.utc_now())
      },
      :retry
    )

    pid = Batcher.Registry.get(p.id)
    :sys.get_state(pid)
    assert is_nil(Repo.get!(Patch, patch.id).manual_approval)
    assert get_in(GitHub.ServerMock.get_state(), [@conn, :pr_build_dispatches]) == [1]
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    :ok = DynamicSupervisor.terminate_child(Batcher.Supervisor, pid)
  end

  test "kim-em's command on a non-main base retains ordinary bors behavior", %{project: p} do
    pr = get_in(GitHub.ServerMock.get_state(), [@conn, :pulls, 1])
    update_repo(:pulls, %{1 => %{pr | base_ref: "pilot"}})
    patch = Syncer.sync_patch(p.id, %{pr | base_ref: "pilot"})

    BorsNG.Command.run(
      %BorsNG.Command{
        project: p,
        patch: patch,
        commenter: %User{user_xref: 477_956, login: "kim-em"}
      },
      :activate
    )

    pid = Batcher.Registry.get(p.id)
    :sys.get_state(pid)
    assert is_nil(Repo.get!(Patch, patch.id).manual_approval)
    assert Repo.exists?(Batch.all_for_project(p.id, :incomplete))
    :ok = DynamicSupervisor.terminate_child(Batcher.Supervisor, pid)
  end

  test "a dispatch failure preserves ordinary admission for a green in-scope PR", %{project: p} do
    update_repo(:pr_build_dispatch_error, {:error, :unavailable})
    patch = manual_review(p)
    assert ManualApproval.valid?(patch)
    assert Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end

  test "candidate bors.toml cannot remove main's scope and build requirements", %{project: p} do
    scope_ci()
    state = GitHub.ServerMock.get_state()

    files =
      get_in(state, [@conn, :files])
      |> Map.put(@head, %{"bors.toml" => "status = []\npr_status = []\n"})

    update_repo(:files, files)
    patch = Syncer.sync_patch(p.id, get_in(state, [@conn, :pulls, 1]))
    Batcher.do_handle_cast({:reviewed, patch.id, "reviewer"}, p.id)
    refute Repo.exists?(Batch.all_for_project(p.id, :incomplete))
  end
end
