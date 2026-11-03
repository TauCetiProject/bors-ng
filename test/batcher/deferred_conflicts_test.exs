defmodule BorsNG.Worker.Batcher.DeferredConflictsTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    Installation,
    LinkPatchBatch,
    OrchestrationJob,
    Patch,
    PatchBundle,
    Project,
    Repo
  }

  alias BorsNG.Worker.{Batcher, CandidateJournal, SourceCompatibility}
  alias BorsNG.Worker.Batcher.{DeferredConflicts, Divider}
  alias BorsNG.GitHub
  @conn {{:installation, 91}, 14}
  @main String.duplicate("b", 40)

  setup do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    previous = System.get_env("BORS_ASYNC_ORCHESTRATION")
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")

    on_exit(fn ->
      if previous,
        do: System.put_env("BORS_ASYNC_ORCHESTRATION", previous),
        else: System.delete_env("BORS_ASYNC_ORCHESTRATION")

      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end)

    installation = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        installation_id: installation.id,
        repo_xref: 14
      })

    source = Repo.insert!(Batch.new(project.id, "main"))

    for n <- 1..3 do
      head = String.pad_leading(to_string(n), 40, "0")
      approval = %{"head_sha" => head, "actor_id" => 7, "merge_base_sha" => @main}

      patch =
        Repo.insert!(%Patch{
          project_id: project.id,
          pr_xref: n,
          commit: head,
          into_branch: "main",
          manual_approval: approval,
          queue_pending: true,
          queue_approval: %{"head_sha" => head, "state" => "approved", "reviewer" => "human"}
        })

      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          batch_id: source.id,
          patch_id: patch.id,
          head_sha: head,
          reviewer: "human",
          manual_approval: approval
        })
      )
    end

    source |> Batch.changeset(%{state: :error}) |> Repo.update!()

    batch =
      Divider.clone_batch(
        Repo.all(LinkPatchBatch.from_batch(source.id)),
        project.id,
        "main",
        "merge_conflict_deferred"
      )

    # Deliberately differ from live main: speculative ancestry is not a
    # sufficient base for sending an individual PR back to repair.
    batch = batch |> Batch.changeset(%{base_sha: String.duplicate("e", 40)}) |> Repo.update!()

    links = Repo.all(LinkPatchBatch.from_batch(batch.id)) |> Enum.sort_by(& &1.patch.pr_xref)
    members = Enum.map(links, &%{patch_id: &1.patch_id, head_sha: &1.head_sha})

    GitHub.ServerMock.put_state(%{
      @conn => %{
        local_git_mode: "local",
        branches: %{"main" => @main},
        pulls:
          Map.new(links, fn l ->
            {l.patch.pr_xref,
             %GitHub.Pr{
               number: l.patch.pr_xref,
               state: :open,
               base_ref: "main",
               head_sha: l.head_sha
             }}
          end),
        labels: %{1 => [], 2 => [], 3 => []},
        comments: %{1 => [], 2 => [], 3 => []},
        statuses: %{}
      }
    })

    %{project: project, batch: batch, links: links, members: members}
  end

  defp checker(statuses) do
    fn _, _, main, members ->
      assert main == @main

      %{
        "status" => "ok",
        "results" =>
          Enum.map(members, fn m ->
            %{
              "pr" => m.pr,
              "head_sha" => m.head_sha,
              "base_sha" => main,
              "policy" => "local-source-v3",
              "status" => Map.get(statuses, m.pr, "pass"),
              "paths" => ["TauCeti/Conflict.lean"]
            }
          end)
      }
    end
  end

  test "confirmed main conflicts use repair, while clean and unknown heads return to the open pool",
       ctx do
    DeferredConflicts.classify(
      ctx.project.id,
      ctx.batch.id,
      ctx.members,
      checker(%{1 => "conflict", 3 => "unknown"})
    )

    [bad | _] = ctx.links
    repaired = Repo.get!(Patch, bad.patch_id)
    assert SourceCompatibility.requested?(repaired, bad.head_sha)
    assert repaired.source_compatibility["repair_base"] == @main
    assert is_nil(repaired.manual_approval)
    assert {:ok, ["needs-rebase"]} = GitHub.get_labels(@conn, 1)
    current = Repo.get!(Batch, ctx.batch.id)
    assert current.state == :waiting
    refute current.sealed
    remaining = Repo.all(LinkPatchBatch.from_batch(current.id))
    assert Enum.sort(Enum.map(remaining, & &1.patch.pr_xref)) == [2, 3]

    for l <- remaining do
      original = Enum.find(ctx.links, &(&1.patch_id == l.patch_id))

      assert Map.take(l, [:head_sha, :reviewer, :manual_approval]) ==
               Map.take(original, [:head_sha, :reviewer, :manual_approval])

      assert l.patch.manual_approval == l.manual_approval
      refute SourceCompatibility.requested?(l.patch, l.head_sha)
    end

    {pool, false} = Batcher.get_new_batch(128, ctx.project.id, "main", 0, false)
    assert pool.id == ctx.batch.id
    assert Repo.exists?(from(j in OrchestrationJob, where: j.kind == "notice"))
    refute Repo.exists?(from(j in OrchestrationJob, where: j.kind == "dispatch"))
  end

  test "head or main changes cannot turn stale conflict evidence into a repair label", ctx do
    check = fn project, conn, main, members ->
      state = GitHub.ServerMock.get_state()

      GitHub.ServerMock.put_state(
        put_in(state, [@conn, :branches, "main"], String.duplicate("c", 40))
      )

      checker(%{1 => "conflict"}).(project, conn, main, members)
    end

    DeferredConflicts.classify(ctx.project.id, ctx.batch.id, ctx.members, check)
    assert length(Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))) == 3

    for l <- ctx.links do
      assert Repo.get!(Patch, l.patch_id).manual_approval == l.manual_approval
      assert {:ok, []} = GitHub.get_labels(@conn, l.patch.pr_xref)
    end
  end

  test "a moved PR head is left queued without withdrawing its old approval", ctx do
    [link | _] = ctx.links
    state = GitHub.ServerMock.get_state()

    pr = %{state[@conn].pulls[1] | head_sha: String.duplicate("d", 40)}
    GitHub.ServerMock.put_state(put_in(state, [@conn, :pulls, 1], pr))

    DeferredConflicts.classify(
      ctx.project.id,
      ctx.batch.id,
      ctx.members,
      checker(%{1 => "conflict"})
    )

    assert Repo.get!(Patch, link.patch_id).manual_approval == link.manual_approval
    assert {:ok, []} = GitHub.get_labels(@conn, 1)
    assert length(Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))) == 3
  end

  test "a running or already constructed batch is never changed", ctx do
    ctx.batch |> Batch.changeset(%{state: :running}) |> Repo.update!()

    DeferredConflicts.classify(ctx.project.id, ctx.batch.id, ctx.members, fn _, _, _, _ ->
      flunk("running")
    end)

    ctx.batch |> Batch.changeset(%{state: :waiting}) |> Repo.update!()
    CandidateJournal.begin(ctx.batch, ctx.links, @main)

    DeferredConflicts.classify(ctx.project.id, ctx.batch.id, ctx.members, fn _, _, _, _ ->
      flunk("constructed")
    end)

    assert length(Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))) == 3
  end

  test "bundles retain their existing indivisible repair policy", ctx do
    [one, two | _] = ctx.links
    bundle = Repo.insert!(%PatchBundle{project_id: ctx.project.id})

    for l <- [one, two],
        do:
          Repo.get!(Patch, l.patch_id)
          |> Ecto.Changeset.change(bundle_id: bundle.id)
          |> Repo.update!()

    DeferredConflicts.classify(ctx.project.id, ctx.batch.id, ctx.members, fn _, _, _, members ->
      assert Enum.map(members, & &1.pr) == [3]
      checker(%{}).(nil, nil, @main, members)
    end)

    for l <- [one, two],
        do: assert(Repo.get!(Patch, l.patch_id).manual_approval == l.manual_approval)

    assert length(Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))) == 3
  end

  test "protected labels retain approval and are not sent to automated repair", ctx do
    state = GitHub.ServerMock.get_state()
    GitHub.ServerMock.put_state(put_in(state, [@conn, :labels, 1], ["human"]))

    DeferredConflicts.classify(
      ctx.project.id,
      ctx.batch.id,
      ctx.members,
      checker(%{1 => "conflict"})
    )

    [first | _] = ctx.links
    assert Repo.get!(Patch, first.patch_id).manual_approval == first.manual_approval
    refute SourceCompatibility.requested?(Repo.get!(Patch, first.patch_id), first.head_sha)
    assert {:ok, ["human"]} = GitHub.get_labels(@conn, 1)
    assert length(Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))) == 3
  end

  test "poll backfill schedules only small idempotent turns for fresh deferred groups", ctx do
    DeferredConflicts.wake(ctx.project)
    DeferredConflicts.wake(ctx.project)
    jobs = Repo.all(from(j in OrchestrationJob, where: like(j.dedup_key, "conflict-triage:%")))
    assert length(jobs) == 1
    [job] = jobs
    assert job.priority == 2

    assert {:triage_deferred_conflicts, id, members} =
             BorsNG.Worker.Orchestration.Outbox.payload(job)

    assert id == ctx.batch.id
    assert length(members) <= 4
  end

  test "backfill rotates past earlier groups even when they remain sealed", ctx do
    batches =
      for n <- 4..8 do
        source = Repo.insert!(Batch.new(ctx.project.id, "main"))
        head = String.pad_leading(to_string(n), 40, "0")

        patch =
          Repo.insert!(%Patch{
            project_id: ctx.project.id,
            pr_xref: n,
            commit: head,
            into_branch: "main"
          })

        link =
          CandidateJournal.insert_member(
            LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
              batch_id: source.id,
              patch_id: patch.id,
              head_sha: head,
              reviewer: "human"
            })
          )
          |> Repo.preload(:patch)

        source |> Batch.changeset(%{state: :error}) |> Repo.update!()
        Divider.clone_batch([link], ctx.project.id, "main", "merge_conflict_deferred")
      end

    DeferredConflicts.wake(ctx.project)
    DeferredConflicts.wake(ctx.project)
    jobs = Repo.all(from(j in OrchestrationJob, where: like(j.dedup_key, "conflict-triage:%")))

    ids =
      MapSet.new(jobs, fn j ->
        {:triage_deferred_conflicts, id, _} = BorsNG.Worker.Orchestration.Outbox.payload(j)
        id
      end)

    assert ids == MapSet.new([ctx.batch.id | Enum.map(batches, & &1.id)])
  end
end
