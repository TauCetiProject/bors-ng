defmodule BorsNG.Worker.FailureIsolationTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    CandidateEvent,
    Installation,
    LinkPatchBatch,
    Patch,
    Project,
    Repo
  }

  alias BorsNG.Worker.{CandidateJournal, FailureIsolation, QueueApproval}
  alias BorsNG.Worker.Batcher.BorsToml
  alias BorsNG.GitHub

  setup do
    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        repo_xref: 14,
        installation_id: inst.id
      })

    batch = Repo.insert!(Batch.new(project.id, "main"))

    Enum.each(1..4, fn n ->
      patch =
        Repo.insert!(%Patch{
          project_id: project.id,
          pr_xref: n,
          commit: "head#{n}",
          into_branch: "main",
          open: true,
          queue_pending: true,
          queue_approval: %{"head_sha" => "head#{n}", "state" => "approved", "reviewer" => "r"}
        })

      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          batch_id: batch.id,
          patch_id: patch.id,
          head_sha: patch.commit,
          reviewer: "r"
        })
      )
    end)

    links = stored_links(batch.id)

    hints = %{
      "complete" => true,
      "changed_files" =>
        Enum.map(
          links,
          &%{
            "pr" => &1.patch.pr_xref,
            "head_sha" => &1.head_sha,
            "paths" => ["TauCeti/File#{&1.patch.pr_xref}.lean"]
          }
        ),
      "import_edges" => [],
      "missing_imports" => [],
      "dependency_paths" => ["TauCeti/Provider.lean"]
    }

    candidate = CandidateJournal.begin(batch, links, "base")

    CandidateJournal.event(candidate, "local_assembled", "bors.local_git", %{
      evidence: %{diagnostic_hints: hints}
    })

    {:ok,
     batch: %{batch | project: project},
     project: project,
     links: links,
     hints: hints,
     candidate: candidate}
  end

  defp stored_links(batch_id) do
    Repo.all(
      from(l in LinkPatchBatch, where: l.batch_id == ^batch_id, order_by: l.id, preload: [:patch])
    )
  end

  defp prs(groups), do: Enum.map(groups, fn g -> Enum.map(g, & &1.patch.pr_xref) end)

  test "a missing provider narrows the importer and leaves all innocent approvals", c do
    hints =
      Map.put(c.hints, "missing_imports", [%{"pr" => 3, "provider_path" => "TauCeti/Absent.lean"}])

    assert {:ok, groups, "missing_import"} = FailureIsolation.partition(c.links, hints, nil)
    assert prs(groups) == [[3], [1, 2, 4]]

    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", %{
      evidence: %{diagnostic_hints: hints}
    })

    assert :retrying = FailureIsolation.split(c.links, c.batch)
    children = Repo.all(from(b in Batch, where: b.id != ^c.batch.id, order_by: b.id))
    assert Enum.all?(children, & &1.sealed)

    assert prs(Enum.map(children, &stored_links(&1.id))) == [
             [3],
             [1, 2, 4]
           ]

    assert Enum.all?(
             Repo.all(Patch),
             &(&1.queue_pending and &1.queue_approval["state"] == "approved")
           )

    # Replay after an executor restart reuses sealed children and snapshots.
    assert :retrying = FailureIsolation.split(c.links, c.batch)
    assert Repo.aggregate(Batch, :count, :id) == 3
  end

  test "import providers and multiple interacting changes stay together", c do
    hints =
      c.hints
      |> Map.put("import_edges", [[1, 3], [3, 4]])
      |> Map.put("missing_imports", [%{"pr" => 1}])

    assert {:ok, groups, "missing_import"} = FailureIsolation.partition(c.links, hints, nil)
    assert prs(groups) == [[1, 3, 4], [2]]

    assert {:ok, groups, "dependency_component"} =
             FailureIsolation.partition(c.links, Map.put(hints, "missing_imports", []), nil)

    assert prs(groups) == [[1, 3, 4], [2]]
  end

  test "a structured conflict separates the offending bundle without breaking it", c do
    [a, b, d, e] = c.links
    a = %{a | patch: %{a.patch | bundle_id: 7}}
    b = %{b | patch: %{b.patch | bundle_id: 7}}

    assert {:ok, groups, "structured_conflict"} =
             FailureIsolation.partition([a, b, d, e], c.hints, %{"pr" => 2})

    assert prs(groups) == [[1, 2], [3, 4]]

    assert :fallback =
             FailureIsolation.partition(
               [a, b],
               %{c.hints | "changed_files" => Enum.take(c.hints["changed_files"], 2)},
               %{"pr" => 2}
             )
  end

  test "unknown evidence, changed heads and non-narrowing groups fall back", c do
    assert :fallback = FailureIsolation.partition(c.links, nil, nil)

    assert :fallback =
             FailureIsolation.partition(c.links, Map.put(c.hints, "complete", false), nil)

    assert :fallback = FailureIsolation.partition(c.links, c.hints, nil)
    [a | rest] = c.links

    assert :fallback =
             FailureIsolation.partition(
               [%{a | patch: %{a.patch | commit: "changed"}} | rest],
               c.hints,
               nil
             )

    hints = Map.put(c.hints, "import_edges", [[1, 2], [2, 3], [3, 4]])
    assert :fallback = FailureIsolation.partition(c.links, hints, nil)
    assert :retrying = FailureIsolation.split(c.links, c.batch)
    children = Repo.all(from(b in Batch, where: b.id != ^c.batch.id, order_by: b.id))

    assert prs(Enum.map(children, &stored_links(&1.id))) == [
             [1, 2],
             [3, 4]
           ]
  end

  test "is_single still isolates an entire unit", c do
    [a, b, d, e] = c.links
    d = %{d | patch: %{d.patch | is_single: true}}
    hints = Map.put(c.hints, "missing_imports", [%{"pr" => 1}])
    assert {:ok, groups, _} = FailureIsolation.partition([a, b, d, e], hints, nil)
    assert prs(groups) == [[1], [3], [2, 4]]
  end

  test "terminal singleton quarantine survives restart and relevant base changes permit ordinary reconsideration",
       c do
    [link | _] = c.links
    # Use a genuine singleton candidate, preserving native terminal semantics.
    batch = Repo.insert!(Batch.new(c.project.id, "main"))

    singleton_link =
      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          patch_id: link.patch_id,
          batch_id: batch.id,
          head_sha: link.head_sha,
          reviewer: "r"
        })
      )

    singleton =
      CandidateJournal.begin(batch, stored_links(batch.id), "base")

    hints = %{c.hints | "changed_files" => [hd(c.hints["changed_files"])]}

    CandidateJournal.event(singleton, "local_assembled", "bors.local_git", %{
      evidence: %{diagnostic_hints: hints}
    })

    assert :failed =
             FailureIsolation.split([%{singleton_link | patch: link.patch}], %{
               batch
               | project: c.project
             })

    Repo.delete_all(from(l in LinkPatchBatch, where: l.batch_id == ^c.batch.id))
    batch |> Batch.changeset(%{state: :error}) |> Repo.update!()
    QueueApproval.fail([link.patch], batch, :build)
    failed = Repo.get!(Patch, link.patch_id)
    assert QueueApproval.blocked?(failed)
    assert failed.queue_approval["quarantine"]["base_sha"] == "base"

    assert Repo.exists?(
             from(e in CandidateEvent,
               where: e.candidate_id == ^singleton.id and e.kind == "head_quarantined"
             )
           )

    conn = {{:installation, 91}, 14}

    GitHub.ServerMock.put_state(%{
      conn => %{
        branches: %{"main" => "newbase"},
        failure_base_changes: %{{"base", "newbase"} => {:ok, ["unrelated.lean"]}}
      }
    })

    d = %{"eligible" => true, "review_safe" => true}
    assert :defer = FailureIsolation.reconsider(conn, failed, d)
    state = GitHub.ServerMock.get_state()

    GitHub.ServerMock.put_state(
      put_in(
        state,
        [conn, :failure_base_changes, {"base", "newbase"}],
        {:ok, ["TauCeti/Provider.lean"]}
      )
    )

    assert {:retry, %{"failure_retry_base" => "newbase"}} =
             FailureIsolation.reconsider(conn, Repo.get!(Patch, link.patch_id), d)

    assert :defer =
             FailureIsolation.reconsider(conn, %{failed | manual_approval: %{"actor_id" => 7}}, d)

    assert :defer = FailureIsolation.reconsider(conn, %{failed | commit: "newhead"}, d)
    assert :defer = FailureIsolation.reconsider(conn, failed, %{d | "review_safe" => false})

    assert :defer =
             FailureIsolation.reconsider(
               conn,
               %{failed | merge_eligibility: %{"failure_retry_base" => "newbase"}},
               d
             )

    capped = %{
      failed
      | merge_eligibility: %{"head_sha" => failed.commit, "failure_retry_count" => 3}
    }

    assert :defer = FailureIsolation.reconsider(conn, capped, d)

    events =
      Repo.all(
        from(e in CandidateEvent,
          where: e.candidate_id == ^singleton.id and e.kind == "quarantine_reconsidered"
        )
      )

    assert length(events) == 1
    # Planning never restores an approval or grants admission itself.
    refute Repo.get!(Patch, failed.id).queue_pending
  end

  test "unknown terminal diagnostics force deterministic bisection even with graph hints", c do
    hints = Map.put(c.hints, "import_edges", [[1, 3]])

    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", %{
      evidence: %{diagnostic_hints: hints}
    })

    CandidateJournal.event(c.candidate, "failure_classified", "bors.trusted_failure", %{
      evidence: %{diagnostic: "unknown_terminal_failure_bisect"}
    })

    assert :retrying = FailureIsolation.split(c.links, c.batch)
    children = Repo.all(from(b in Batch, where: b.id != ^c.batch.id, order_by: b.id))

    assert prs(Enum.map(children, &stored_links(&1.id))) == [
             [1, 2],
             [3, 4]
           ]
  end

  test "restart and unordered database reads retain frozen candidate split order", c do
    CandidateJournal.begin(c.batch, Enum.reverse(c.links), "base")
    # Simulate a crash after committing only the first half. Replaying a
    # different SQL row order must create exactly the original second half.
    BorsNG.Worker.Batcher.Divider.clone_batch(
      Enum.take(Enum.reverse(c.links), 2),
      c.project.id,
      "main",
      "bisection"
    )

    shuffled = Enum.drop(c.links, 1) ++ Enum.take(c.links, 1)
    assert :retrying = FailureIsolation.split(shuffled, c.batch)
    children = Repo.all(from(b in Batch, where: b.id != ^c.batch.id, order_by: b.id))
    assert prs(Enum.map(children, &stored_links(&1.id))) == [[4, 3], [2, 1]]

    assert :retrying =
             FailureIsolation.split(Enum.drop(c.links, 1) ++ Enum.take(c.links, 1), c.batch)

    assert Repo.aggregate(Batch, :count, :id) == 3
  end

  test "quarantine cannot bind diagnostic completeness to a changed head", c do
    [link | _] = c.links
    q = FailureIsolation.quarantine(%{link.patch | commit: "newhead"}, c.batch, :build)
    assert q["head_sha"] == link.head_sha
    refute q["diagnostics_complete"]

    changed =
      link.patch
      |> Patch.changeset(%{
        commit: "newhead",
        queue_approval: %{"state" => "approved", "head_sha" => "newhead"}
      })
      |> Repo.update!()

    QueueApproval.fail([changed], c.batch, :build)
    assert Repo.get!(Patch, changed.id).queue_approval["state"] == "approved"
  end

  test "feature off restores legacy conflict isolation and skips diagnostic hints", c do
    System.put_env("BORS_FAILURE_DIAGNOSTICS", "off")
    on_exit(fn -> System.delete_env("BORS_FAILURE_DIAGNOSTICS") end)
    conn = {{:installation, 91}, 14}

    GitHub.ServerMock.put_state(%{
      conn => %{
        pulls:
          Map.new(c.links, fn l ->
            {l.patch.pr_xref,
             %GitHub.Pr{number: l.patch.pr_xref, mergeable: l.patch.pr_xref != 3}}
          end)
      }
    })

    assert :retrying = FailureIsolation.split(c.links, c.batch, %{})
    children = Repo.all(from(b in Batch, where: b.id != ^c.batch.id, order_by: b.id))

    assert prs(Enum.map(children, &stored_links(&1.id))) == [
             [3],
             [1, 2, 4]
           ]
  end

  test "classification is bound to the exact generation and ignores stale commits", c do
    CandidateJournal.bind(c.candidate, "staged", "tree", "base", %BorsToml{status: ["build"]}, %{
      content: nil
    })

    batch = %{c.batch | commit: "staged", base_sha: "base"}
    FailureIsolation.classified(batch, "infrastructure", %{run_id: 5, conclusion: "cancelled"})
    FailureIsolation.classified(%{batch | commit: "obsolete"}, "candidate_failure")
    [event] = Repo.all(from(e in CandidateEvent, where: e.kind == "failure_classified"))
    assert event.details["classification"] == "infrastructure"
    assert event.details["evidence"]["run_id"] == 5
    refute event.details["proof_authority"]
  end
end
