defmodule BorsNG.Worker.MergeEligibilityTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Batch, Installation, LinkPatchBatch, Patch, Project, Repo}
  alias BorsNG.GitHub
  alias BorsNG.Worker.{Batcher, MergeEligibility}

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
    update_repo(:labels, %{1 => ["human"]})
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
end
