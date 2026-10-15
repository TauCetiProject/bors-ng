defmodule BorsNG.Worker.SourceCompatibilityTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    Installation,
    LinkPatchBatch,
    OrchestrationJob,
    Patch,
    Project,
    Repo,
    Status
  }

  alias BorsNG.Worker.{CandidateJournal, SourceCompatibility}
  alias BorsNG.GitHub
  @conn {{:installation, 91}, 14}
  @head String.duplicate("a", 40)
  @base String.duplicate("b", 40)

  setup do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    on_exit(fn -> :sys.resume(BorsNG.Worker.Orchestration.Dispatcher) end)
    old = System.get_env("BORS_ASYNC_ORCHESTRATION")
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")

    on_exit(fn ->
      if old,
        do: System.put_env("BORS_ASYNC_ORCHESTRATION", old),
        else: System.delete_env("BORS_ASYNC_ORCHESTRATION")
    end)

    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        installation_id: inst.id,
        repo_xref: 14
      })

    batch = Repo.insert!(Batch.new(project.id, "main"))

    patches =
      for n <- 1..2 do
        p =
          Repo.insert!(%Patch{
            project_id: project.id,
            pr_xref: n,
            commit: @head,
            into_branch: "main",
            queue_pending: true,
            queue_approval: %{"head_sha" => @head, "state" => "approved", "reviewer" => "r"}
          })

        CandidateJournal.insert_member(
          LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
            patch_id: p.id,
            batch_id: batch.id,
            head_sha: @head,
            reviewer: "r"
          })
        )

        p
      end

    GitHub.ServerMock.put_state(%{
      @conn => %{
        local_git_mode: "local",
        branches: %{"main" => @base},
        pulls:
          Map.new(
            1..2,
            &{&1, %GitHub.Pr{number: &1, state: :open, base_ref: "main", head_sha: @head}}
          ),
        labels: %{1 => [], 2 => []},
        comments: %{1 => [], 2 => []}
      }
    })

    {:ok, project: project, batch: batch, patches: patches}
  end

  test "assembly-sized compatibility input uses independent bounded requests", ctx do
    extra =
      for n <- 3..65 do
        Repo.insert!(%Patch{
          project_id: ctx.project.id,
          pr_xref: n,
          commit: @head,
          into_branch: "main"
        })
      end

    mock = GitHub.ServerMock.get_state()

    pulls =
      Map.new(
        1..65,
        &{&1, %GitHub.Pr{number: &1, state: :open, base_ref: "main", head_sha: @head}}
      )

    GitHub.ServerMock.put_state(put_in(mock, [@conn, :pulls], pulls))

    checker = fn _, _, base, members ->
      assert base == @base
      assert length(members) <= 32
      send(self(), {:chunk, Enum.map(members, & &1.pr)})
      # A failure in the middle chunk cannot discard results from its siblings.
      if hd(members).pr == 33 do
        %{"status" => "error", "details" => %{"kind" => "timeout"}}
      else
        %{"status" => "ok", "results" => Enum.map(members, &result(&1.pr, "pass"))}
      end
    end

    SourceCompatibility.check(ctx.project, @conn, @base, ctx.patches ++ extra, checker)
    assert_received {:chunk, first}
    assert_received {:chunk, second}
    assert_received {:chunk, third}
    assert first == Enum.to_list(1..32)
    assert second == Enum.to_list(33..64)
    assert third == [65]
    refute_received {:chunk, _}

    for patch <- ctx.patches ++ extra do
      actual = Repo.get!(Patch, patch.id).source_compatibility
      assert actual["status"] == if(patch.pr_xref in 33..64, do: "unknown", else: "pass")
    end
  end

  defp result(pr, status \\ "missing_source") do
    %{
      "pr" => pr,
      "head_sha" => @head,
      "base_sha" => @base,
      "policy" => "local-source-v3",
      "status" => status,
      "coverage" => "all_project_local_headers",
      "missing_imports" => [
        %{
          "path" => "TauCeti/Consumer.lean",
          "module" => "TauCeti.Removed",
          "provider_path" => "TauCeti/Removed.lean"
        }
      ]
    }
  end

  defp update_mock(key, value),
    do:
      GitHub.ServerMock.get_state()
      |> put_in([@conn, key], value)
      |> GitHub.ServerMock.put_state()

  test "exact failure produces one existing head request and admission preserves innocent approval",
       c do
    [bad, healthy] = c.patches

    update_mock(:files, %{
      "main" => %{
        "bors.toml" =>
          "status = [\"build\"]\n[labels]\non_queue = \"ready-to-merge\"\nbuilding = \"bors-staging\"\n"
      }
    })

    update_mock(:labels, %{1 => ["ready-to-merge"], 2 => ["ready-to-merge"]})

    SourceCompatibility.apply_results(c.project, @conn, @base, c.patches, [
      result(1),
      result(2, "pass")
    ])

    assert SourceCompatibility.requested?(Repo.get!(Patch, bad.id), @head)
    refute SourceCompatibility.requested?(Repo.get!(Patch, healthy.id), @head)
    assert [%{payload: payload}] = Repo.all(OrchestrationJob)
    {_conn, 1, body, marker} = :erlang.binary_to_term(payload, [:safe])
    assert String.starts_with?(body, "Merge-queue recovery for head `")
    assert body =~ @base and body =~ "TauCeti/Consumer.lean" and body =~ "TauCeti.Removed"
    assert marker == "<!--tauceti-rebase:v1 #{@head}-->"
    SourceCompatibility.apply_results(c.project, @conn, @base, c.patches, [result(1)])
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 1
    before = Repo.get!(Patch, healthy.id).queue_approval
    links = Repo.all(LinkPatchBatch.from_batch(c.batch.id))
    assert [remaining] = SourceCompatibility.admit(c.project, @conn, c.batch, links, @base)
    assert remaining.patch_id == healthy.id
    assert {:ok, ["needs-rebase"]} = GitHub.get_labels(@conn, 1)
    assert {:ok, ["ready-to-merge"]} = GitHub.get_labels(@conn, 2)
    assert Repo.get!(Patch, healthy.id).queue_approval == before
    assert Repo.aggregate(Status, :count, :id) == 0
  end

  test "unknown coverage and multi-member ambiguity produce no request", c do
    SourceCompatibility.apply_results(c.project, @conn, @base, c.patches, [result(1, "unknown")])
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0

    assert SourceCompatibility.admit(
             c.project,
             @conn,
             c.batch,
             Repo.all(LinkPatchBatch.from_batch(c.batch.id)),
             @base
           )
           |> length() == 2
  end

  test "a prior checker timeout is retried lazily without changing the source identity", c do
    patches =
      Enum.map(c.patches, fn patch ->
        old = result(patch.pr_xref, "unknown") |> Map.put("policy", "local-source-v2")
        patch |> Ecto.Changeset.change(source_compatibility: old) |> Repo.update!()
      end)

    SourceCompatibility.check(c.project, @conn, @base, patches, fn _, _, base, members ->
      assert base == @base
      assert Enum.map(members, & &1.head_sha) == [@head, @head]
      %{"status" => "ok", "results" => Enum.map(members, &result(&1.pr, "pass"))}
    end)

    for patch <- patches do
      assert %{
               "policy" => "local-source-v3",
               "status" => "pass",
               "head_sha" => @head,
               "base_sha" => @base
             } = Repo.get!(Patch, patch.id).source_compatibility
    end
  end

  test "a helper timeout persists exact unknown coverage without a hold or repeated source work",
       c do
    SourceCompatibility.check(c.project, @conn, @base, c.patches, fn _, _, _, _ ->
      %{
        "status" => "error",
        "details" => %{"kind" => "timeout", "operation" => "fetch", "phase" => "fetch"}
      }
    end)

    current = Enum.map(c.patches, &Repo.get!(Patch, &1.id))

    Enum.each(current, fn patch ->
      assert %{
               "status" => "unknown",
               "reason" => "source_helper_timeout",
               "operation" => "fetch",
               "phase" => "fetch",
               "base_sha" => @base,
               "head_sha" => @head
             } = patch.source_compatibility

      refute SourceCompatibility.requested?(patch, @head)
    end)

    SourceCompatibility.check(c.project, @conn, @base, current, fn _, _, _, _ ->
      flunk("unknown coverage must fall back to authoritative staging rather than spin")
    end)

    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0

    assert SourceCompatibility.admit(
             c.project,
             @conn,
             c.batch,
             Repo.all(LinkPatchBatch.from_batch(c.batch.id)),
             @base
           )
           |> length() == 2
  end

  test "a completed local assembly permits one retry of a same-input transient unknown", c do
    checker = fn _, _, _, _ ->
      %{
        "status" => "error",
        "details" => %{"kind" => "timeout", "phase" => "fetch", "operation" => "fetch"}
      }
    end

    SourceCompatibility.check(c.project, @conn, @base, c.patches, checker)
    cached = Enum.map(c.patches, &Repo.get!(Patch, &1.id))

    SourceCompatibility.check(c.project, @conn, @base, cached, fn _, _, _, _ ->
      flunk("same evidence must stay cached before a new Git readiness hint")
    end)

    links = Repo.all(LinkPatchBatch.from_batch(c.batch.id))
    candidate = CandidateJournal.begin(c.batch, links, @base)
    CandidateJournal.event(candidate, "local_assembled", "bors.local_git", %{commit: @head})

    SourceCompatibility.check(c.project, @conn, @base, cached, fn _, _, base, members ->
      assert base == @base
      send(self(), {:retried, Enum.map(members, &{&1.pr, &1.head_sha})})
      checker.(nil, nil, nil, nil)
    end)

    assert_received {:retried, [{1, @head}, {2, @head}]}
    still_unknown = Enum.map(c.patches, &Repo.get!(Patch, &1.id))

    SourceCompatibility.check(c.project, @conn, @base, still_unknown, fn _, _, _, _ ->
      flunk("a single assembly hint must not create an endless retry loop")
    end)

    for patch <- still_unknown do
      assert patch.source_compatibility["status"] == "unknown"
      refute SourceCompatibility.requested?(patch, @head)
    end

    # Reconstructing and publishing an exact local candidate can warm the mirror
    # without another main commit or a new PR head.
    CandidateJournal.event(candidate, "staging_pushed", "bors.local_git", %{commit: @head})

    SourceCompatibility.check(c.project, @conn, @base, still_unknown, fn _, _, base, members ->
      assert base == @base
      %{"status" => "ok", "results" => Enum.map(members, &result(&1.pr, "pass"))}
    end)

    passed = Enum.map(c.patches, &Repo.get!(Patch, &1.id))
    assert Enum.all?(passed, &(&1.source_compatibility["status"] == "pass"))
    CandidateJournal.event(candidate, "local_assembled", "bors.local_git", %{commit: @head})

    SourceCompatibility.check(c.project, @conn, @base, passed, fn _, _, _, _ ->
      flunk("a cache hint must not repeat passing validation for identical inputs")
    end)
  end

  test "old readiness hints stay consumed across restart and other events do not retrigger", c do
    candidate =
      CandidateJournal.begin(c.batch, Repo.all(LinkPatchBatch.from_batch(c.batch.id)), @base)

    CandidateJournal.event(candidate, "local_assembled", "bors.local_git", %{commit: @head})

    SourceCompatibility.check(c.project, @conn, @base, c.patches, fn _, _, _, _ ->
      %{"status" => "error", "details" => %{"kind" => "timeout"}}
    end)

    cached = Enum.map(c.patches, &Repo.get!(Patch, &1.id))
    CandidateJournal.event(candidate, "local_assembled", "bors.github", %{})
    CandidateJournal.event(candidate, "source_compatibility_checked", "bors.local_git", %{})

    SourceCompatibility.check(Repo.get!(Project, c.project.id), @conn, @base, cached, fn _,
                                                                                         _,
                                                                                         _,
                                                                                         _ ->
      flunk("a restarted consumer must not retry from an old or unrelated readiness hint")
    end)
  end

  test "ambiguous or bounded non-transient unknowns are not blanket retried after assembly", c do
    SourceCompatibility.check(c.project, @conn, @base, c.patches, fn _, _, _, members ->
      %{
        "status" => "ok",
        "results" =>
          Enum.map(members, fn m ->
            result(m.pr, "unknown")
            |> Map.put(
              "reason",
              if(m.pr == 1, do: "ambiguous_header", else: "source_helper_output_limit")
            )
          end)
      }
    end)

    candidate =
      CandidateJournal.begin(c.batch, Repo.all(LinkPatchBatch.from_batch(c.batch.id)), @base)

    CandidateJournal.event(candidate, "local_assembled", "bors.local_git", %{commit: @head})

    SourceCompatibility.check(
      c.project,
      @conn,
      @base,
      Enum.map(c.patches, &Repo.get!(Patch, &1.id)),
      fn _, _, _, _ ->
        flunk("assembly cannot resolve ambiguous syntax or change fixed limits")
      end
    )
  end

  test "a fetched local conflict can unblock a bootstrap unknown without becoming guilt itself",
       c do
    SourceCompatibility.check(c.project, @conn, @base, c.patches, fn _, _, _, _ ->
      %{"status" => "error", "details" => %{"kind" => "cold_mirror_unavailable"}}
    end)

    cached = Enum.map(c.patches, &Repo.get!(Patch, &1.id))

    candidate =
      CandidateJournal.begin(c.batch, Repo.all(LinkPatchBatch.from_batch(c.batch.id)), @base)

    CandidateJournal.event(candidate, "local_conflict", "bors.local_git", %{})
    assert Enum.all?(cached, &(not SourceCompatibility.requested?(&1, @head)))

    SourceCompatibility.check(c.project, @conn, @base, cached, fn _, _, _, members ->
      %{"status" => "ok", "results" => Enum.map(members, &result(&1.pr))}
    end)

    assert Enum.all?(c.patches, &SourceCompatibility.requested?(Repo.get!(Patch, &1.id), @head))
  end

  test "Git work for another repository cannot consume this repository retry budget", c do
    SourceCompatibility.check(c.project, @conn, @base, c.patches, fn _, _, _, _ ->
      %{"status" => "error", "details" => %{"kind" => "timeout"}}
    end)

    other =
      Repo.insert!(%Project{
        name: "Other/Project",
        installation_id: c.project.installation_id,
        repo_xref: 15
      })

    batch = Repo.insert!(Batch.new(other.id, "main"))
    candidate = CandidateJournal.begin(batch, [], @base)
    CandidateJournal.event(candidate, "local_assembled", "bors.local_git", %{})

    SourceCompatibility.check(
      c.project,
      @conn,
      @base,
      Enum.map(c.patches, &Repo.get!(Patch, &1.id)),
      fn _, _, _, _ ->
        flunk("unrelated repository work is not a readiness hint")
      end
    )
  end

  test "the bounded background cursor retries a transient once after a new Git hint", c do
    [first, second] = c.patches
    second |> Ecto.Changeset.change(open: false) |> Repo.update!()

    expire = fn ->
      project = Repo.get!(Project, c.project.id)
      state = Map.put(project.source_compatibility || %{}, "checked_at", 0)
      project |> Ecto.Changeset.change(source_compatibility: state) |> Repo.update!()
    end

    fail = fn _, _, _, _ ->
      %{"status" => "error", "details" => %{"kind" => "timeout"}}
    end

    SourceCompatibility.chunk(c.project.id, fail)
    old = Repo.get!(Patch, first.id)
    assert old.source_compatibility["status"] == "unknown"
    # Legacy records are also bounded, rather than retried on every restart.
    old
    |> Ecto.Changeset.change(
      source_compatibility: Map.delete(old.source_compatibility, "source_retry_epoch")
    )
    |> Repo.update!()

    expire.()
    SourceCompatibility.chunk(c.project.id, fn _, _, _, _ -> flunk("no new Git hint") end)

    links =
      Repo.all(LinkPatchBatch.from_batch(c.batch.id)) |> Enum.filter(&(&1.patch_id == first.id))

    candidate = CandidateJournal.begin(c.batch, links, @base)
    CandidateJournal.event(candidate, "staging_pushed", "bors.local_git", %{commit: @head})
    expire.()

    SourceCompatibility.chunk(c.project.id, fn _, _, base, members ->
      assert base == @base
      assert Enum.map(members, &{&1.pr, &1.head_sha}) == [{1, @head}]
      send(self(), :background_retry)
      fail.(nil, nil, nil, nil)
    end)

    assert_received :background_retry
    expire.()
    SourceCompatibility.chunk(c.project.id, fn _, _, _, _ -> flunk("old consumed hint") end)
    assert Repo.get!(Patch, first.id).source_compatibility["status"] == "unknown"
  end

  test "helper failure cannot overwrite a result after main moves", c do
    SourceCompatibility.check(c.project, @conn, @base, c.patches, fn _, _, _, _ ->
      update_mock(:branches, %{"main" => String.duplicate("d", 40)})
      %{"status" => "error", "details" => %{"kind" => "helper_unavailable"}}
    end)

    assert Enum.all?(c.patches, &(Repo.get!(Patch, &1.id).source_compatibility == nil))
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0
  end

  test "new head and new main cannot accept an old result", c do
    [patch | _] = c.patches
    update_mock(:branches, %{"main" => String.duplicate("c", 40)})
    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    assert Repo.get!(Patch, patch.id).source_compatibility == nil
    update_mock(:branches, %{"main" => @base})

    update_mock(:pulls, %{
      1 => %GitHub.Pr{number: 1, state: :open, head_sha: String.duplicate("d", 40)}
    })

    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    assert Repo.get!(Patch, patch.id).source_compatibility == nil
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0
  end

  test "hold labels preserve ownership boundary and requests cannot authorize a successor head",
       c do
    [patch | _] = c.patches
    update_mock(:labels, %{1 => ["human", "keep"], 2 => []})
    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    refute SourceCompatibility.requested?(Repo.get!(Patch, patch.id), @head)
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0

    assert SourceCompatibility.requested?(
             %{patch | source_compatibility: %{"repair_head" => @head}},
             @head
           )

    refute SourceCompatibility.requested?(
             %{patch | source_compatibility: %{"repair_head" => @head}},
             String.duplicate("c", 40)
           )
  end

  test "only conclusive individual compiler evidence requests semantic repair", c do
    [patch, _] = c.patches
    all = Repo.all(LinkPatchBatch.from_batch(c.batch.id))
    [link] = Enum.filter(all, &(&1.patch_id == patch.id))
    Enum.each(Enum.reject(all, &(&1.patch_id == patch.id)), &Repo.delete!/1)
    candidate = CandidateJournal.begin(c.batch, [link], @base)

    batch = %{
      c.batch
      | candidate_id: candidate.id,
        commit: String.duplicate("c", 40),
        base_sha: @base
    }

    evidence = %{
      conclusion: "failure",
      run_id: 42,
      failed_steps: [
        %{name: "Build exact candidate under bwrap with workflow-pinned audits, offline"}
      ]
    }

    CandidateJournal.bind(
      candidate,
      batch.commit,
      String.duplicate("d", 40),
      @base,
      %BorsNG.Worker.Batcher.BorsToml{status: ["build"]},
      %{}
    )

    BorsNG.Worker.FailureIsolation.classified(batch, "candidate_failure", evidence)
    SourceCompatibility.proven_failure(batch, all, @conn)
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0
    SourceCompatibility.proven_failure(batch, [link], @conn)
    assert [%{kind: "notice", payload: payload}] = Repo.all(OrchestrationJob)
    {_, _, body, _} = :erlang.binary_to_term(payload, [:safe])

    assert body =~ "actions/runs/42" and
             body =~ "Authoritative exact single-PR integrated build failed"

    assert SourceCompatibility.requested?(Repo.get!(Patch, patch.id), @head)
  end

  test "durable cursor rotates across main advances and stale work cannot publish", c do
    for n <- 3..6 do
      Repo.insert!(%Patch{
        project_id: c.project.id,
        pr_xref: n,
        commit: @head,
        into_branch: "main",
        queue_pending: true
      })
    end

    update_mock(
      :pulls,
      Map.new(
        1..6,
        &{&1, %GitHub.Pr{number: &1, state: :open, base_ref: "main", head_sha: @head}}
      )
    )

    checker = fn _, _, base, members ->
      send(self(), {:checked, Enum.map(members, & &1.pr)})

      %{
        "status" => "ok",
        "results" => Enum.map(members, &Map.merge(result(&1.pr, "pass"), %{"base_sha" => base}))
      }
    end

    SourceCompatibility.chunk(c.project.id, checker)
    assert_receive {:checked, [1]}
    cursor = Repo.get!(Project, c.project.id).source_compatibility["cursor"]
    assert cursor == Repo.get_by!(Patch, project_id: c.project.id, pr_xref: 1).id
    new_main = String.duplicate("c", 40)
    update_mock(:branches, %{"main" => new_main})
    SourceCompatibility.wake(c.project, new_main)
    SourceCompatibility.chunk(c.project.id, checker)
    assert_receive {:checked, [2]}

    assert Repo.get_by!(Patch, project_id: c.project.id, pr_xref: 2).source_compatibility[
             "base_sha"
           ] == new_main

    # Resume from the persisted cursor. A main move during the bounded work
    # discards results without discarding fair progress.
    next_main = String.duplicate("d", 40)
    SourceCompatibility.wake(c.project, next_main)

    checker = fn _, _, base, members ->
      update_mock(:branches, %{"main" => next_main})

      %{
        "status" => "ok",
        "results" => Enum.map(members, &Map.merge(result(&1.pr), %{"base_sha" => base}))
      }
    end

    SourceCompatibility.chunk(c.project.id, checker)
    assert Repo.aggregate(OrchestrationJob |> where([j], j.kind == "notice"), :count, :id) == 0

    assert Repo.get_by!(Patch, project_id: c.project.id, pr_xref: 3).source_compatibility == nil

    assert Repo.get!(Project, c.project.id).source_compatibility["cursor"] != cursor
  end

  test "exact decoded identities skip fresh rows and bound metadata traversal", c do
    for n <- 3..20 do
      Repo.insert!(%Patch{
        project_id: c.project.id,
        pr_xref: n,
        commit: @head,
        into_branch: "main",
        queue_pending: true
      })
    end

    for patch <- Repo.all(from(p in Patch, where: p.project_id == ^c.project.id)) do
      if patch.pr_xref != 17,
        do:
          patch
          |> Ecto.Changeset.change(source_compatibility: result(patch.pr_xref, "pass"))
          |> Repo.update!()
    end

    SourceCompatibility.chunk(c.project.id, fn _, _, _, _ ->
      flunk("the first 16 metadata rows all have exact cached coverage")
    end)

    project = Repo.get!(Project, c.project.id)

    assert project.source_compatibility["cursor"] ==
             Repo.get_by!(Patch, project_id: c.project.id, pr_xref: 16).id

    project
    |> Ecto.Changeset.change(
      source_compatibility: Map.put(project.source_compatibility, "checked_at", 0)
    )
    |> Repo.update!()

    update_mock(:pulls, %{
      17 => %GitHub.Pr{number: 17, state: :open, base_ref: "main", head_sha: @head}
    })

    SourceCompatibility.chunk(c.project.id, fn _, _, _, members ->
      assert [%{pr: 17}] = members
      %{"status" => "ok", "results" => [result(17, "pass")]}
    end)

    assert Repo.get_by!(Patch, project_id: c.project.id, pr_xref: 17).source_compatibility[
             "status"
           ] == "pass"

    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0
  end

  test "restored source on a new main retires only the local-source request without a push", c do
    [patch | _] = c.patches
    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    assert SourceCompatibility.requested?(Repo.get!(Patch, patch.id), @head)
    new_main = String.duplicate("d", 40)
    update_mock(:branches, %{"main" => new_main})
    passing = Map.merge(result(1, "pass"), %{"base_sha" => new_main})
    SourceCompatibility.apply_results(c.project, @conn, new_main, [patch], [passing])
    refute SourceCompatibility.requested?(Repo.get!(Patch, patch.id), @head)
    assert Repo.get!(Patch, patch.id).commit == @head
    assert GitHub.get_labels(@conn, 1) == {:ok, []}
    assert [%{state: "done"}] = Repo.all(OrchestrationJob)
  end

  test "held evidence skips background Git and a released hold reuses the finding", c do
    [patch, healthy] = c.patches
    update_mock(:labels, %{1 => ["keep"], 2 => []})

    SourceCompatibility.apply_results(c.project, @conn, @base, c.patches, [
      result(1),
      result(2, "pass")
    ])

    SourceCompatibility.chunk(c.project.id, fn _, _, _, _ ->
      flunk("a held exact finding and healthy cached head need no source scan")
    end)

    current = Repo.get!(Patch, patch.id)

    SourceCompatibility.check(c.project, @conn, @base, [current], fn _, _, _, _ ->
      flunk("holding the diagnosed head must not repeat Git")
    end)

    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0
    update_mock(:labels, %{1 => [], 2 => []})

    SourceCompatibility.check(c.project, @conn, @base, [Repo.get!(Patch, patch.id)], fn _,
                                                                                        _,
                                                                                        _,
                                                                                        _ ->
      flunk("releasing the hold must reuse the exact diagnosis")
    end)

    repaired = Repo.get!(Patch, patch.id)
    assert SourceCompatibility.requested?(repaired, @head)
    refute Map.has_key?(repaired.source_compatibility, "hold_head")
    refute Map.has_key?(repaired.source_compatibility, "action")
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 1
    refute SourceCompatibility.requested?(Repo.get!(Patch, healthy.id), @head)
  end

  test "incomplete request resolution retries from cached pass without rescanning source", c do
    [patch | _] = c.patches
    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    new_main = String.duplicate("d", 40)
    update_mock(:branches, %{"main" => new_main})
    update_mock(:resolve_source_request_error, :incomplete_request_history)
    passing = Map.put(result(1, "pass"), "base_sha", new_main)
    SourceCompatibility.apply_results(c.project, @conn, new_main, [patch], [passing])
    current = Repo.get!(Patch, patch.id)
    assert current.source_compatibility["action"] =~ "maintainer attention"
    assert SourceCompatibility.requested?(current, @head)

    checker = fn _, _, _, _ -> flunk("the exact passing inventory must be reused") end
    SourceCompatibility.check(c.project, @conn, new_main, [current], checker)
    assert SourceCompatibility.requested?(Repo.get!(Patch, patch.id), @head)

    update_mock(:resolve_source_request_error, nil)
    SourceCompatibility.check(c.project, @conn, new_main, [Repo.get!(Patch, patch.id)], checker)
    resolved = Repo.get!(Patch, patch.id)
    refute SourceCompatibility.requested?(resolved, @head)
    refute Map.has_key?(resolved.source_compatibility, "action")
    assert GitHub.get_labels(@conn, 1) == {:ok, []}
  end

  test "releasing a hold on an already requested head clears stale hold metadata", c do
    [patch | _] = c.patches
    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    update_mock(:labels, %{1 => ["keep"], 2 => []})
    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    held = Repo.get!(Patch, patch.id)
    assert held.source_compatibility["hold_head"] == @head
    update_mock(:labels, %{1 => [], 2 => []})

    SourceCompatibility.check(c.project, @conn, @base, [held], fn _, _, _, _ ->
      flunk("an existing exact request must not repeat source work")
    end)

    current = Repo.get!(Patch, patch.id)
    assert SourceCompatibility.requested?(current, @head)
    refute Map.has_key?(current.source_compatibility, "hold_head")
    refute Map.has_key?(current.source_compatibility, "action")
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 1
  end

  test "cached held findings cannot move an automatic bump out of its dedicated path", c do
    [patch | _] = c.patches
    user = Repo.insert!(%BorsNG.Database.User{login: "tauceti-review-bot[bot]", user_xref: 77})

    patch =
      patch
      |> Ecto.Changeset.change(
        author_id: user.id,
        head_ref: "bump-mathlib/example",
        source_compatibility: Map.put(result(1), "hold_head", @head)
      )
      |> Repo.update!()

    assert BorsNG.Worker.BumpRecovery.automatic?(patch)

    SourceCompatibility.check(c.project, @conn, @base, [patch], fn _, _, _, _ ->
      flunk("automatic bumps retain their dedicated recovery path")
    end)

    refute SourceCompatibility.requested?(Repo.get!(Patch, patch.id), @head)
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0
  end

  test "partial label publication is retried without duplicating the request or source work", c do
    [patch | _] = c.patches
    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    assert :ok = GitHub.remove_label(@conn, 1, "needs-rebase")
    current = Repo.get!(Patch, patch.id)

    current =
      current
      |> Ecto.Changeset.change(
        source_compatibility: Map.put(current.source_compatibility, "projection_pending", true)
      )
      |> Repo.update!()

    SourceCompatibility.check(c.project, @conn, @base, [current], fn _, _, _, _ ->
      flunk("a pending label projection must reuse the exact diagnosis")
    end)

    assert {:ok, ["needs-rebase"]} = GitHub.get_labels(@conn, 1)
    refute Repo.get!(Patch, patch.id).source_compatibility["projection_pending"]
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 1
  end

  test "a new head supersedes stored repair and projection metadata", c do
    [patch | _] = c.patches
    SourceCompatibility.apply_results(c.project, @conn, @base, [patch], [result(1)])
    new_head = String.duplicate("e", 40)
    current = Repo.get!(Patch, patch.id) |> Patch.changeset(%{commit: new_head}) |> Repo.update!()

    update_mock(:pulls, %{
      1 => %GitHub.Pr{number: 1, state: :open, base_ref: "main", head_sha: new_head}
    })

    SourceCompatibility.apply_results(c.project, @conn, @base, [current], [
      Map.put(result(1, "pass"), "head_sha", new_head)
    ])

    updated = Repo.get!(Patch, patch.id)
    refute SourceCompatibility.requested?(updated, new_head)
    refute Map.has_key?(updated.source_compatibility, "repair_head")
    refute Map.has_key?(updated.source_compatibility, "projection_pending")
  end

  test "promotion and heartbeat wakeups coalesce durably", c do
    Enum.each(1..5, fn _ -> SourceCompatibility.wake(c.project, @base) end)
    SourceCompatibility.wake(c.project)
    assert [%{kind: "compatibility"}] = Repo.all(OrchestrationJob)
    assert Repo.get!(Project, c.project.id).source_compatibility["required_sha"] == @base
  end
end
