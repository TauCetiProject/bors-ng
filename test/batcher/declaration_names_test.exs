defmodule BorsNG.Worker.DeclarationNamesTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.DeclarationNames

  defp link(id, pr, bundle \\ nil),
    do: %{
      id: id,
      patch_id: id,
      head_sha: String.pad_leading(to_string(pr), 40, "0"),
      patch: %{pr_xref: pr, bundle_id: bundle}
    }

  defp known(added), do: %{"state" => "known", "added" => added}

  test "a name added by two units in different modules defers the later unit, naming both" do
    [a, b, c] = [link(1, 10), link(2, 20), link(3, 30)]

    records = %{
      1 => known(%{"TauCeti.M" => ["TauCeti.x", "TauCeti.only_a"]}),
      2 => known(%{"TauCeti.N" => ["TauCeti.x"]}),
      3 => known(%{"TauCeti.P" => ["TauCeti.y"]})
    }

    assert [skip] = DeclarationNames.collisions([[a], [b], [c]], records)
    assert skip["members"] == [%{"pr" => 20, "head_sha" => b.head_sha}]

    assert skip["conflict"] == %{
             "pr" => 20,
             "head_sha" => b.head_sha,
             "kind" => "declaration_collision",
             "name" => "TauCeti.x",
             "module" => "TauCeti.N",
             "other_pr" => 10,
             "other_module" => "TauCeti.M"
           }
  end

  test "the same name in the same module, unknown records and bundles are not collisions" do
    [a, b, c, d, e] = [link(1, 10), link(2, 20), link(3, 30), link(4, 40, 7), link(5, 50, 7)]

    records = %{
      1 => known(%{"TauCeti.M" => ["TauCeti.x"]}),
      # same module: a merge conflict or compile error, handled elsewhere
      2 => known(%{"TauCeti.M" => ["TauCeti.x"]}),
      # unknown record: never checked, never indexed
      3 => %{"state" => "unknown", "reason" => "records=0"},
      # bundle members with a colliding name: bundles are not checked
      4 => known(%{"TauCeti.Q" => ["TauCeti.x"]}),
      5 => known(%{"TauCeti.R" => ["TauCeti.x"]})
    }

    assert DeclarationNames.collisions([[a], [b], [c], [d, e]], records) == []
    # an empty record is known and simply adds nothing
    assert DeclarationNames.collisions([[a], [b]], %{1 => %{"state" => "empty", "added" => %{}}}) ==
             []
  end

  test "a skipped unit does not index its names, so a third unit is judged against accepted ones" do
    [a, b, c] = [link(1, 10), link(2, 20), link(3, 30)]

    records = %{
      1 => known(%{"TauCeti.M" => ["TauCeti.x"]}),
      2 => known(%{"TauCeti.N" => ["TauCeti.x", "TauCeti.z"]}),
      3 => known(%{"TauCeti.P" => ["TauCeti.z"]})
    }

    # b collides with a on x; c adds z, which only the skipped b added: no collision
    assert [%{"conflict" => %{"pr" => 20}}] =
             DeclarationNames.collisions([[a], [b], [c]], records)
  end

  test "an exhausted budget leaves the remaining units unchecked instead of deferring them" do
    [a, b, c] = [link(1, 10), link(2, 20), link(3, 30)]

    records = %{
      1 => known(%{"TauCeti.M" => ["TauCeti.x"]}),
      2 => known(%{"TauCeti.N" => ["TauCeti.x"]}),
      3 => known(%{"TauCeti.P" => ["TauCeti.x"]})
    }

    {:ok, clock} = Agent.start_link(fn -> [0, 0, 5_000, 5_000] end)

    now = fn :millisecond ->
      Agent.get_and_update(clock, fn [t | rest] -> {t, rest ++ [t]} end)
    end

    # first unit indexed within budget; the clock then jumps past 2 s
    assert DeclarationNames.collisions([[a], [b], [c]], records, now) == []
  end

  test "validate accepts only the exact shapes and bounds" do
    assert {:ok, %{"state" => "known"}} =
             DeclarationNames.validate(%{
               "state" => "known",
               "added" => %{"TauCeti.M" => ["TauCeti.x"]}
             })

    assert {:ok, %{"state" => "empty"}} =
             DeclarationNames.validate(%{"state" => "empty", "added" => %{}})

    assert {:ok, %{"state" => "incomplete"}} =
             DeclarationNames.validate(%{"state" => "incomplete", "reason" => "producer"})

    for bad <- [
          %{"state" => "known", "added" => %{}},
          %{"state" => "empty", "added" => %{"TauCeti.M" => ["TauCeti.x"]}},
          %{"state" => "known", "added" => %{"TauCeti.M" => [1]}},
          %{"state" => "known", "added" => %{"TauCeti.M" => "TauCeti.x"}},
          %{"state" => "known", "added" => %{"TauCeti.M" => [""]}},
          %{"state" => "weird"},
          nil
        ] do
      assert {:ok, %{"state" => "unknown"}} = DeclarationNames.validate(bad)
    end
  end

  test "build_run accepts only the newest successful build status with a trusted run URL" do
    repo = "TauCetiProject/TauCeti"

    ok = %{
      "context" => "build",
      "state" => "success",
      "target_url" => "https://github.com/#{repo}/actions/runs/41"
    }

    assert DeclarationNames.build_run([ok], repo) == {:lookup, 41}
    # newest first: a later failure hides an older success
    assert DeclarationNames.build_run([%{ok | "state" => "failure"}, ok], repo) == :unavailable

    for bad <- [
          %{ok | "target_url" => "https://github.com/other/repo/actions/runs/41"},
          %{ok | "target_url" => "http://github.com/#{repo}/actions/runs/41"},
          %{ok | "target_url" => "https://github.com/#{repo}/actions/runs/41?x=1"},
          %{ok | "target_url" => "https://github.com/#{repo}/actions/runs/abc"},
          %{ok | "context" => "scope"},
          Map.delete(ok, "target_url")
        ] do
      assert DeclarationNames.build_run([bad], repo) == :unavailable
    end

    assert DeclarationNames.build_run(nil, repo) == :unavailable
  end
end

defmodule BorsNG.Worker.DeclarationNamesFetchTest do
  use BorsNG.Database.ModelCase

  alias BorsNG.Database.{Batch, CandidateAdmission, Installation, LinkPatchBatch, Patch, Project}
  alias BorsNG.GitHub
  alias BorsNG.Worker.{CandidateJournal, DeclarationNames}

  setup do
    installation = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        installation_id: installation.id,
        repo_xref: 14
      })

    batch = Repo.insert!(Batch.new(project.id, "main"))
    head = String.duplicate("a", 40)

    patch =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: 42,
        commit: head,
        into_branch: "main",
        manual_approval: %{"head_sha" => head, "actor_id" => 7}
      })

    link =
      %LinkPatchBatch{}
      |> LinkPatchBatch.changeset(%{
        batch_id: batch.id,
        patch_id: patch.id,
        head_sha: head,
        reviewer: "human",
        manual_approval: patch.manual_approval
      })
      |> CandidateJournal.insert_member()

    {:ok, project: project, batch: batch, patch: patch, link: link, head: head}
  end

  test "a fresh admission has no record, and an unavailable build run stores unknown", c do
    admission = CandidateJournal.admission(c.link.id)
    assert is_nil(admission.snapshot["declaration_names"])
    # scheduling is a no-op unless recording is switched on (and never raises)
    assert :ok = DeclarationNames.schedule(c.project.id, [c.link])
    assert DeclarationNames.mode() == "off"

    GitHub.ServerMock.put_state(%{{{:installation, 91}, 14} => %{}})
    assert :ok = DeclarationNames.fetch(c.project.id, admission.id)

    assert %{"state" => "unknown", "reason" => "build_run_unavailable"} =
             CandidateJournal.admission(c.link.id).snapshot["declaration_names"]

    # never fetched again for the same head
    GitHub.ServerMock.put_state(%{
      {{:installation, 91}, 14} => %{pr_build_jobs: %{c.head => %{job_id: 5}}}
    })

    assert :ok = DeclarationNames.fetch(c.project.id, admission.id)

    assert CandidateJournal.admission(c.link.id).snapshot["declaration_names"]["reason"] ==
             "build_run_unavailable"
  end

  test "recording mode queues one dedup fetch job per recordless admission and nothing otherwise",
       c do
    alias BorsNG.Database.OrchestrationJob

    old =
      {System.get_env("BORS_ASYNC_ORCHESTRATION"), System.get_env("BORS_DECLARATION_COLLISIONS")}

    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")

    on_exit(fn ->
      for {key, value} <-
            Enum.zip(
              ["BORS_ASYNC_ORCHESTRATION", "BORS_DECLARATION_COLLISIONS"],
              Tuple.to_list(old)
            ) do
        if value, do: System.put_env(key, value), else: System.delete_env(key)
      end
    end)

    System.delete_env("BORS_DECLARATION_COLLISIONS")
    assert :ok = DeclarationNames.schedule(c.project.id, [c.link])
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0

    System.put_env("BORS_DECLARATION_COLLISIONS", "record")
    assert DeclarationNames.recording?() and not DeclarationNames.enabled?()
    assert :ok = DeclarationNames.schedule(c.project.id, [c.link])
    assert :ok = DeclarationNames.schedule(c.project.id, [c.link])
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 1

    # a stored record means nothing more to queue
    admission = CandidateJournal.admission(c.link.id)

    admission
    |> Ecto.Changeset.change(
      snapshot: Map.put(admission.snapshot, "declaration_names", %{"state" => "unknown"})
    )
    |> Repo.update!()

    Repo.delete_all(OrchestrationJob)
    assert :ok = DeclarationNames.schedule(c.project.id, [c.link])
    assert Repo.aggregate(OrchestrationJob, :count, :id) == 0
  end

  test "a retry copies the record of the same head and a stored record reaches later retries",
       c do
    admission = CandidateJournal.admission(c.link.id)
    record = %{"state" => "known", "added" => %{"TauCeti.M" => ["TauCeti.x"]}}

    admission
    |> Ecto.Changeset.change(snapshot: Map.put(admission.snapshot, "declaration_names", record))
    |> Repo.update!()

    retry = Repo.insert!(Batch.new(c.project.id, "main"))

    retried =
      %LinkPatchBatch{}
      |> LinkPatchBatch.changeset(%{
        batch_id: retry.id,
        patch_id: c.patch.id,
        head_sha: c.head,
        reviewer: "human",
        manual_approval: c.patch.manual_approval
      })
      |> CandidateJournal.insert_member(c.link, "merge_conflict_deferred")

    assert CandidateJournal.admission(retried.id).snapshot["declaration_names"] == record
    assert DeclarationNames.records([retried]) == %{retried.id => record}

    # a record for another head is never used for this link
    assert DeclarationNames.records([%{retried | head_sha: String.duplicate("b", 40)}]) == %{}
    assert Repo.aggregate(CandidateAdmission, :count, :id) == 2
  end
end
