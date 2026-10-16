defmodule BorsNG.Worker.CandidateJournalTest do
  use BorsNG.Database.ModelCase

  alias BorsNG.Database.{
    Batch,
    Candidate,
    CandidateAdmission,
    CandidateEvent,
    Installation,
    LinkPatchBatch,
    Patch,
    Project
  }

  alias BorsNG.Worker.CandidateJournal, as: Journal
  alias BorsNG.Worker.Batcher.{BorsToml, Divider}

  setup do
    installation = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "example/project",
        installation_id: installation.id,
        repo_xref: 14
      })

    batch = Repo.insert!(Batch.new(project.id, "main"))

    patch =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: 42,
        commit: "head",
        into_branch: "main",
        manual_approval: %{"head_sha" => "head", "actor_id" => 7}
      })

    {:ok, project: project, batch: batch, patch: patch}
  end

  defp member(batch, patch) do
    %LinkPatchBatch{}
    |> LinkPatchBatch.changeset(%{
      batch_id: batch.id,
      patch_id: patch.id,
      head_sha: patch.commit,
      reviewer: "reviewer",
      manual_approval: patch.manual_approval
    })
    |> Journal.insert_member()
  end

  defp begin(batch),
    do: Journal.begin(batch, Repo.all(LinkPatchBatch.from_batch(batch.id)), "base")

  defp bind(candidate, commit),
    do:
      Journal.bind(candidate, commit, "tree", "base", %BorsToml{status: ["build"]}, %{
        content: nil,
        unknown_reason: "read failed"
      })

  test "admission and candidate snapshots survive changed head, withdrawal and batch deletion",
       ctx do
    link = member(ctx.batch, ctx.patch)
    admission = Journal.admission(link.id)
    candidate = begin(ctx.batch)
    assert admission.snapshot["original_admitted_at"] > 0
    assert candidate.snapshot["members"] |> hd() |> Map.get("head_sha") == "head"
    ctx.patch |> Patch.changeset(%{commit: "new", manual_approval: nil}) |> Repo.update!()
    Repo.delete!(link)
    Repo.delete!(ctx.batch)
    assert Repo.get!(Candidate, candidate.id) == candidate
    assert Repo.get!(CandidateAdmission, admission.id) == admission

    assert Journal.diagnostics(ctx.project.id, "main", ctx.batch.id) |> hd() |> Map.get(:snapshot) ==
             candidate.snapshot
  end

  test "restart allocates a new immutable generation and rejects rebinding", ctx do
    member(ctx.batch, ctx.patch)
    first = begin(ctx.batch)
    bind(first, "old")
    second = begin(Repo.get!(Batch, ctx.batch.id))
    bind(second, "new")
    assert second.generation == first.generation + 1
    assert second.snapshot["previous_generation_id"] == first.id
    assert Journal.commit_binding(first)["commit"] == "old"
    assert_raise ArgumentError, "candidate already bound", fn -> bind(first, "rewritten") end
    assert Journal.commit_binding(first)["commit"] == "old"
  end

  test "retry ancestry preserves approval identity and replay appends nothing", ctx do
    BorsNG.Worker.Batcher.Diversity.remember(ctx.patch, ["roadmap/A"])
    link = member(ctx.batch, ctx.patch)
    parent = begin(ctx.batch)
    ctx.batch |> Batch.changeset(%{state: :error}) |> Repo.update!()
    links = Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))
    BorsNG.Worker.Batcher.Diversity.remember(ctx.patch, ["roadmap/B"])
    child = Divider.clone_batch(links, ctx.project.id, "main", "inconclusive_ci")
    child_link = Repo.one!(LinkPatchBatch.from_batch(child.id))
    a = Journal.admission(child_link.id)
    original = Journal.admission(link.id)
    assert a.snapshot["parent_batch_id"] == ctx.batch.id
    assert a.snapshot["parent_candidate_id"] == parent.id
    assert a.snapshot["parent_admission_id"] == original.id
    assert a.snapshot["original_admitted_at"] == original.snapshot["original_admitted_at"]
    assert a.snapshot["retry_reason"] == "inconclusive_ci"
    assert a.snapshot["manual_approval"] == original.snapshot["manual_approval"]
    assert a.snapshot["scheduling"] == original.snapshot["scheduling"]
    assert a.snapshot["scheduling"]["label_hint"]["labels"] == ["roadmap/A"]
    assert Divider.clone_batch(links, ctx.project.id, "main").id == child.id
    assert Repo.aggregate(CandidateAdmission, :count, :id) == 2
    assert Journal.admission(child_link.id) == a
  end

  test "one bulk read orders live unchanged members and excludes withdrawn history", ctx do
    link = member(ctx.batch, ctx.patch)
    other = Repo.insert!(Batch.new(ctx.project.id, "main"))
    patch = Repo.insert!(%Patch{project_id: ctx.project.id, pr_xref: 43, commit: "other"})
    other_link = member(other, patch)
    now = System.system_time(:microsecond)
    assert Journal.queue_age_members([ctx.batch, other]) |> length() == 2
    rows = Journal.queue_age_members([ctx.batch, other])
    keys = BorsNG.Worker.Batcher.QueueAge.keys([ctx.batch, other], rows, now)
    assert elem(keys[ctx.batch.id], 1) == :original_admission
    assert elem(keys[other.id], 1) == :original_admission
    Repo.delete!(link)

    assert Journal.queue_age_members([ctx.batch, other]) |> Enum.map(fn {l, _, _} -> l.id end) ==
             [other_link.id]

    assert Journal.admission(link.id) != nil
    patch |> Patch.changeset(%{commit: "changed"}) |> Repo.update!()
    keys = BorsNG.Worker.Batcher.QueueAge.keys([other], Journal.queue_age_members([other]), now)
    assert elem(keys[other.id], 1) == :poll_fallback
    assert Journal.queue_age_members([%{other | state: :running}]) == []
  end

  test "scheduler metadata is one bulk read and deferred while serial running", ctx do
    member(ctx.batch, ctx.patch)
    repo = :persistent_term.get(:db_repo)
    event = repo.config()[:telemetry_prefix] ++ [:query]
    ref = make_ref()

    :telemetry.attach(
      ref,
      event,
      fn _, _, metadata, {owner, ref} ->
        if String.contains?(metadata.query, "candidate_admissions"), do: send(owner, ref)
      end,
      {self(), ref}
    )

    on_exit(fn -> :telemetry.detach(ref) end)
    assert {:waiting, [batch]} = BorsNG.Worker.Batcher.sort_batches([ctx.batch])
    assert batch.id == ctx.batch.id
    assert_received ^ref
    refute_received ^ref
    running = %{ctx.batch | id: -1, state: :running, priority: 100}
    assert {:running, _} = BorsNG.Worker.Batcher.sort_batches([running, ctx.batch])
    refute_received ^ref
    assert {:running, _} = BorsNG.Worker.Batcher.sort_batches([running, ctx.batch], :successor)
    assert_received ^ref
    refute_received ^ref
  end

  test "multiple sealed retry generations retain identity and exhaust FIFO inheritance", ctx do
    link = member(ctx.batch, ctx.patch)
    original = Journal.admission(link.id)

    Enum.reduce(1..5, ctx.batch, fn generation, parent ->
      parent |> Batch.changeset(%{state: :error, priority: 100}) |> Repo.update!()

      child =
        Divider.clone_batch(
          Repo.all(LinkPatchBatch.from_batch(parent.id)),
          ctx.project.id,
          "main"
        )

      child_link = Repo.one!(LinkPatchBatch.from_batch(child.id))
      admission = Journal.admission(child_link.id)
      assert child.sealed
      assert child.priority == ctx.patch.priority
      assert admission.snapshot["queue_age_retries"] == min(generation, 3)

      assert admission.snapshot["original_admitted_at"] ==
               original.snapshot["original_admitted_at"]

      assert child_link.head_sha == link.head_sha
      assert child_link.reviewer == link.reviewer
      assert child_link.manual_approval == link.manual_approval
      rows = Journal.queue_age_members([child])
      keys = BorsNG.Worker.Batcher.QueueAge.keys([child], rows, System.system_time(:microsecond))

      assert elem(keys[child.id], 1) ==
               if(generation < 3, do: :original_admission, else: :retry_fairness)

      child
    end)
  end

  test "scheduling evidence is expendable and never changes approval identity", ctx do
    BorsNG.Worker.Batcher.Diversity.remember(ctx.patch, ["roadmap/A"])
    link = member(ctx.batch, ctx.patch)
    a = Journal.admission(link.id)
    assert a.snapshot["scheduling"]["label_hint"]["head_sha"] == link.head_sha
    assert a.snapshot["scheduling"]["priority"] == ctx.patch.priority
    assert a.snapshot["manual_approval"] == ctx.patch.manual_approval
    Process.delete(BorsNG.Worker.Batcher.Diversity)
    candidate = begin(ctx.batch)
    assert hd(candidate.snapshot["members"])["scheduling"] == a.snapshot["scheduling"]
  end

  test "historical rows retain explicitly unknown admissions and ancestry", ctx do
    Repo.insert!(%LinkPatchBatch{
      batch_id: ctx.batch.id,
      patch_id: ctx.patch.id,
      reviewer: "reviewer",
      head_sha: nil
    })

    assert Repo.aggregate(CandidateAdmission, :count, :id) == 0
    candidate = begin(ctx.batch)
    [member] = candidate.snapshot["members"]
    assert member["source"] == "legacy.unknown"
    assert member["original_admitted_at"] == nil
    assert member["parent_candidate_id"] == nil
    assert member["head_sha"] == nil
    assert member["assembly_head_sha"] == "head"
    [detail] = Journal.diagnostics(ctx.project.id, "main", ctx.batch.id)
    assert detail.runner_started_at == nil
    assert detail.runner_finished_at == nil
  end

  test "delayed status cannot annotate a later generation with a different commit", ctx do
    member(ctx.batch, ctx.patch)
    first = begin(ctx.batch)
    bind(first, "old")
    old_batch = %{ctx.batch | commit: "old"}
    Journal.status(old_batch, "build", :ok, "github.status_webhook")
    second = begin(ctx.batch)
    bind(second, "new")
    Journal.status(old_batch, "build", :error, "github.status_webhook")

    assert Repo.aggregate(
             from(e in CandidateEvent, where: e.kind == "status_observed"),
             :count,
             :id
           ) == 1

    Journal.status(%{ctx.batch | commit: "new"}, "build", :ok, "github.status_poll")

    event =
      Repo.one!(
        from(e in CandidateEvent,
          where: e.candidate_id == ^second.id and e.kind == "status_observed"
        )
      )

    assert event.source == "github.status_poll"
    assert event.details["commit"] == "new"
    assert event.details["generation_binding"] == "commit_only"
    assert event.details["runner_lifecycle_authority"] == false
    assert event.observed_at >= second.observed_at
  end

  test "same SHA across retries has separate identity and explicitly commit-only evidence", ctx do
    member(ctx.batch, ctx.patch)
    first = begin(ctx.batch)
    bind(first, "same")
    second = begin(ctx.batch)
    bind(second, "same")
    refute first.id == second.id
    Journal.status(%{ctx.batch | commit: "same"}, "build", :running, "github.status_webhook")
    event = Repo.one!(from(e in CandidateEvent, where: e.kind == "status_observed"))
    assert event.candidate_id == second.id
    assert event.details["generation_binding"] == "commit_only"
  end

  test "speculative predecessor must bind the exact repository branch and base", ctx do
    member(ctx.batch, ctx.patch)
    predecessor = begin(ctx.batch)
    bind(predecessor, "predecessor-head")
    next_batch = Repo.insert!(Batch.new(ctx.project.id, "main"))
    candidate = Journal.begin(next_batch, [], "predecessor-head", predecessor.id)
    assert candidate.snapshot["speculative_predecessor_id"] == predecessor.id

    assert_raise ArgumentError, fn ->
      Journal.begin(next_batch, [], "wrong-base", predecessor.id)
    end

    other_branch = Repo.insert!(Batch.new(ctx.project.id, "pilot"))

    assert_raise ArgumentError, fn ->
      Journal.begin(other_branch, [], "predecessor-head", predecessor.id)
    end

    other_project = Repo.insert!(%Project{repo_xref: 15})
    other_batch = Repo.insert!(Batch.new(other_project.id, "main"))

    assert_raise ArgumentError, fn ->
      Journal.begin(other_batch, [], "predecessor-head", predecessor.id)
    end
  end

  test "digest hashes toolchain bytes", _ do
    assert Journal.digest("abc") ==
             "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
  end

  test "check lifecycle retains authoritative GitHub times and rejects stale commits", ctx do
    member(ctx.batch, ctx.patch)
    candidate = begin(ctx.batch)
    bind(candidate, "built")
    ctx.batch |> Batch.changeset(%{commit: "built", state: :running}) |> Repo.update!()

    check = %{
      "id" => 123,
      "head_sha" => "built",
      "name" => "build",
      "status" => "completed",
      "started_at" => "2026-10-14T00:00:00Z",
      "completed_at" => "2026-10-14T00:02:00Z",
      "app" => %{"id" => 15368},
      "check_suite" => %{"id" => 456}
    }

    Journal.observe_check(ctx.project.id, check)
    Journal.observe_check(ctx.project.id, %{check | "head_sha" => "stale"})
    event = Repo.one!(from(e in CandidateEvent, where: e.kind == "check_run_observed"))
    assert event.details["check_run_id"] == 123
    assert event.details["check_finished_at"] - event.details["check_started_at"] == 120_000_000
    assert event.details["timestamp_source"] == "github.check_run"
    assert event.details["runner_lifecycle_authority"] == false
    assert event.details["generation_binding"] == "commit_only"
    assert event.observed_at > event.details["check_finished_at"]
    assert Repo.get!(Batch, ctx.batch.id).state == :running
  end

  test "assembler result validates identity and rejects abandoned generations", ctx do
    member(ctx.batch, ctx.patch)
    first = begin(ctx.batch)
    assembler = BorsNG.Worker.CandidateAssembler
    result = Map.merge(assembler.identity(first), %{commit: "built", tree: "tree"})
    assert assembler.validate_result(first, result) == :ok

    assert assembler.validate_result(first, %{result | base_sha: "other"}) ==
             {:error, :identity_mismatch}

    begin(ctx.batch)
    assert assembler.validate_result(first, result) == {:error, :stale_candidate}

    assert_raise ArgumentError, "assembly result rejected: stale_candidate", fn ->
      bind(first, "built")
    end

    assert Journal.commit_binding(first) == %{}
  end

  test "assembly result cannot bind after a head change or partial withdrawal", ctx do
    link = member(ctx.batch, ctx.patch)
    candidate = begin(ctx.batch)
    assembler = BorsNG.Worker.CandidateAssembler
    result = Map.merge(assembler.identity(candidate), %{commit: "built", tree: "tree"})
    ctx.patch |> Patch.changeset(%{commit: "changed"}) |> Repo.update!()
    assert assembler.validate_result(candidate, result) == {:error, :membership_changed}
    Repo.get!(Patch, ctx.patch.id) |> Patch.changeset(%{commit: "head"}) |> Repo.update!()
    assert assembler.validate_result(candidate, result) == :ok
    Repo.delete!(link)
    assert assembler.validate_result(candidate, result) == {:error, :membership_changed}

    assert_raise ArgumentError, "assembly result rejected: membership_changed", fn ->
      bind(candidate, "built")
    end

    assert Journal.commit_binding(candidate) == %{}
  end
end
