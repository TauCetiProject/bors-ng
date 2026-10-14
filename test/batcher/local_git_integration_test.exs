defmodule BorsNG.Worker.LocalGitIntegrationTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    CandidateEvent,
    Installation,
    LinkPatchBatch,
    Patch,
    Project,
    Repo,
    Status
  }

  alias BorsNG.Worker.{Batcher, CandidateJournal}
  alias BorsNG.GitHub
  @conn {{:installation, 91}, 14}
  @base String.duplicate("b", 40)
  @head String.duplicate("a", 40)
  @tree String.duplicate("c", 40)
  @commit String.duplicate("d", 40)

  setup do
    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        installation_id: inst.id,
        repo_xref: 14
      })

    batch = Repo.insert!(%Batch{Batch.new(project.id, "main") | last_polled: 0})
    approval = %{"actor_id" => 7, "head_sha" => @head, "merge_base_sha" => @base}

    patch =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: 1,
        commit: @head,
        into_branch: "main",
        manual_approval: approval
      })

    link =
      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          patch_id: patch.id,
          batch_id: batch.id,
          head_sha: @head,
          reviewer: "human",
          manual_approval: approval
        })
      )

    toml = "status = [\"build\"]\nuse_squash_merge = true\n"

    GitHub.ServerMock.put_state(%{
      @conn => %{
        local_git_mode: "local",
        backend_snapshot: {:ok, %{backend: "bors", github_count: 0}},
        branches: %{"main" => @base, "staging" => @base},
        commits: %{},
        comments: %{1 => []},
        statuses: %{},
        labels: %{1 => []},
        files: %{@base => %{"bors.toml" => toml}, "main" => %{"bors.toml" => toml}},
        pulls: %{
          1 => %GitHub.Pr{
            number: 1,
            title: "Title",
            body: "Body",
            state: :open,
            base_ref: "main",
            head_sha: @head,
            user: %GitHub.User{id: 6, login: "author"}
          }
        },
        pr_commits: %{
          1 => [
            %GitHub.Commit{
              sha: @head,
              author_name: "Author",
              author_email: "author@example.invalid"
            }
          ]
        }
      }
    })

    # Substitute only the process boundary. Real Git plumbing is independently
    # exercised by local_git_test.py, including transport and stale leases.
    dir = Path.join(System.tmp_dir!(), "bors-helper-test-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    python = System.find_executable("python3")

    helper = """
    #!#{python}
    import json,os,sys
    request=json.load(open(sys.argv[2]))
    kind=os.environ.get('BORS_TEST_HELPER_FAILURE','')
    if kind == request['operation']:
        print(json.dumps(dict(status='error',details=dict(kind='network_unavailable'))))
    elif request['operation']=='assemble':
        print(json.dumps(dict(status='ok',result=request['identity'] | dict(commit='#{@commit}',tree='#{@tree}'),commits=[],git_version='test',mirror_bytes=1)))
    else:
        print(json.dumps(dict(status='ok')))
    """

    File.write!(Path.join(dir, "python3"), helper)
    File.chmod!(Path.join(dir, "python3"), 0o755)
    old_path = System.get_env("PATH")
    System.put_env("PATH", dir <> ":" <> old_path)

    on_exit(fn ->
      System.put_env("PATH", old_path)
      System.delete_env("BORS_TEST_HELPER_FAILURE")
      File.rm_rf!(dir)
    end)

    {:ok, project: project, batch: batch, patch: patch, link: link}
  end

  defp events(batch),
    do:
      Repo.all(
        from(e in CandidateEvent,
          where: e.candidate_id == ^CandidateJournal.latest(batch.id).id
        )
      )

  test "local result binds exact tree/base and authorizes the existing CI path", ctx do
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :running
    assert batch.commit == @commit
    assert batch.base_sha == @base
    binding = CandidateJournal.commit_binding(CandidateJournal.latest(batch.id))
    assert binding["tree"] == @tree
    assert binding["base_sha"] == @base
    assert Repo.one!(Status.all_for_batch(batch.id)).state == :running
    assert Enum.any?(events(batch), &(&1.kind == "constructed" and &1.source == "bors.local_git"))
    assert Enum.any?(events(batch), &(&1.kind == "staging_pushed"))
    refute Map.has_key?(GitHub.ServerMock.get_state()[@conn].branches, "staging.tmp")
    assert Repo.get!(LinkPatchBatch, ctx.link.id).head_sha == @head
  end

  test "running shadow checks GitHub tree once without replacing CI identity", ctx do
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    candidate = CandidateJournal.latest(batch.id)
    state = GitHub.ServerMock.get_state()
    state = put_in(state, [@conn, :local_git_mode], "shadow")
    state = put_in(state, [@conn, :commit_trees], %{@commit => @tree})
    state = put_in(state, [@conn, :statuses, @commit], %{"build" => :running})
    GitHub.ServerMock.put_state(state)
    batch |> Batch.changeset(%{last_polled: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, batch.id)
    assert batch.commit == @commit
    assert batch.state == :running
    assert CandidateJournal.latest(batch.id).id == candidate.id
    compared = Enum.filter(events(batch), &(&1.kind == "shadow_compared"))
    assert length(compared) == 1
    assert hd(compared).details["matches"]
    batch |> Batch.changeset(%{last_polled: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert length(Enum.filter(events(batch), &(&1.kind == "shadow_compared"))) == 1
  end

  test "failed assembly retains exact approvals and never arms or pushes CI", ctx do
    System.put_env("BORS_TEST_HELPER_FAILURE", "assemble")
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    assert batch.state == :waiting
    assert batch.commit == nil
    assert batch.recovery_after > DateTime.to_unix(DateTime.utc_now())
    assert Repo.all(Status.all_for_batch(batch.id)) == []
    assert Repo.get!(LinkPatchBatch, ctx.link.id).manual_approval == ctx.link.manual_approval
    assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
  end

  test "failed push retains intent and restart reconstructs a new generation", ctx do
    System.put_env("BORS_TEST_HELPER_FAILURE", "publish")
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    first = CandidateJournal.latest(batch.id)
    assert batch.state == :waiting
    assert batch.commit == nil
    assert Repo.one!(Status.all_for_batch(batch.id)).state == :running
    assert Enum.any?(events(batch), &(&1.kind == "local_push_error"))
    System.delete_env("BORS_TEST_HELPER_FAILURE")
    batch |> Batch.changeset(%{last_polled: 0, recovery_after: 0}) |> Repo.update!()
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    assert Repo.get!(Batch, batch.id).state == :running
    assert CandidateJournal.latest(batch.id).generation == first.generation + 1
    assert Repo.get!(LinkPatchBatch, ctx.link.id).manual_approval == ctx.link.manual_approval
  end

  test "changed approved head cannot enter local assembly or staging", ctx do
    state = GitHub.ServerMock.get_state()
    pr = %{state[@conn].pulls[1] | head_sha: String.duplicate("e", 40)}
    GitHub.ServerMock.put_state(put_in(state, [@conn, :pulls, 1], pr))
    Batcher.handle_info({:poll, :once}, ctx.project.id)
    batch = Repo.get!(Batch, ctx.batch.id)
    refute batch.state == :running
    assert GitHub.ServerMock.get_state()[@conn].branches["staging"] == @base
    refute Enum.any?(events(batch), &(&1.kind == "local_assembled"))
  end
end
