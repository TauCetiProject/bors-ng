defmodule BorsNG.Worker.FailureIsolationTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    CandidateEvent,
    Installation,
    LinkPatchBatch,
    Patch,
    PatchBundle,
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

  defp terminal(c) do
    CandidateJournal.bind(
      c.candidate,
      "staged",
      "tree",
      "base",
      %BorsToml{status: ["build"]},
      %{}
    )

    batch = %{c.batch | commit: "staged", base_sha: "base"}

    evidence = %{
      run_id: 100,
      run_attempt: 1,
      run_created_at: DateTime.utc_now() |> DateTime.add(1, :second) |> DateTime.to_iso8601(),
      conclusion: "failure",
      workflow: ".github/workflows/pr-build.yml",
      identity: "bors branch=main batch=#{batch.id} head=staged base=base",
      failed_steps: [
        %{
          job_id: 200,
          job_name: "sandboxed-build",
          name: "Build exact candidate under bwrap with workflow-pinned audits, offline"
        }
      ]
    }

    {batch, evidence}
  end

  defp log_result(paths, kind \\ "compiler_error") do
    %{
      "complete" => true,
      "jobs" => [%{"job_id" => 200, "observations" => [%{"kind" => kind, "paths" => paths}]}]
    }
  end

  defp parsed_log_result(text) do
    script = """
    import importlib.util, json, sys
    spec = importlib.util.spec_from_file_location('diagnostics', 'priv/failure_diagnostics.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    print(json.dumps(module.parse(sys.argv[1])))
    """

    {output, 0} =
      System.cmd("python3", [
        "-c",
        script,
        text
      ])

    %{"complete" => true, "jobs" => [%{"job_id" => 200, "observations" => Jason.decode!(output)}]}
  end

  defp environment_result(findings) do
    parsed_log_result(
      "LINT-ENV-LOCATIONS " <> Jason.encode!(%{version: 1, complete: true, findings: findings})
    )
  end

  defp provider_terminal(c) do
    {batch, evidence} = terminal(c)
    identity = BorsNG.Worker.CandidateAssembler.identity(c.candidate)

    provenance =
      Map.merge(identity, %{
        commit: "staged",
        tree: "tree",
        members: Enum.map(c.candidate.snapshot["members"], &Map.take(&1, ["pr", "head_sha"]))
      })

    hints =
      c.hints
      |> Map.put("ownership_complete", true)
      |> Map.put("provenance", provenance)
      |> Jason.encode!()
      |> Jason.decode!()

    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", %{
      commit: "staged",
      tree: "tree",
      evidence: %{diagnostic_hints: hints}
    })

    {batch, evidence, hints}
  end

  defp provider_result(binding, paths, providers \\ ["TauCeti/File3.lean"]) do
    %{
      "complete" => true,
      "binding" => binding,
      "authority" => "retry_only",
      "proof_authority" => false,
      "files" =>
        Enum.map(paths, fn path ->
          %{
            "path" => path,
            "blob" => String.duplicate("a", 40),
            "bytes" => 100,
            "providers" =>
              Enum.map(providers, &%{"path" => &1, "blob" => String.duplicate("b", 40)})
          }
        end)
    }
  end

  defp bounded_result(binding, roots, providers \\ ["TauCeti/File3.lean"]) do
    expanded =
      Enum.map(
        roots,
        &%{
          "path" => &1,
          "blob" => String.duplicate("a", 40),
          "base_blob" => String.duplicate("a", 40),
          "bytes" => 100,
          "depth" => 0,
          "providers" => providers
        }
      )

    frontier =
      Enum.map(
        providers,
        &%{
          "path" => &1,
          "blob" => String.duplicate("b", 40),
          "bytes" => 50,
          "depth" => 1,
          "stop" => "changed_frontier"
        }
      )

    %{
      "complete" => false,
      "bounded_complete" => true,
      "policy" => "bounded_depth_v1",
      "full_closure_complete" => false,
      "binding" => binding,
      "roots" => roots,
      "nodes" => expanded ++ frontier,
      "residual_frontier" => [],
      "changed_frontier" => providers,
      "headers" => length(roots),
      "source_bytes" => 100 * length(roots),
      "elapsed_ms" => 10,
      "authority" => "retry_only",
      "proof_authority" => false,
      "limits" => %{
        "max_depth" => 5,
        "max_headers" => 64,
        "max_file_bytes" => 262_144,
        "max_source_bytes" => 524_288,
        "max_paths" => 128,
        "max_evidence_bytes" => 65_536,
        "total_timeout_ms" => 5000
      }
    }
  end

  defp checkpoint_error(binding, roots, providers \\ ["TauCeti/File3.lean"]) do
    direct = provider_result(binding, roots, providers)

    files =
      Enum.map(direct["files"], fn file ->
        file
        |> Map.put("base_blob", file["blob"])
        |> Map.update!("providers", fn values -> Enum.map(values, &Map.put(&1, "bytes", 50)) end)
      end)

    checkpoint =
      Map.merge(direct, %{
        "policy" => "direct_depth_v1",
        "files" => files,
        "headers" => length(roots),
        "source_bytes" => 100 * length(roots),
        "paths" => Enum.sort(Enum.uniq(roots ++ providers)),
        "completed_elapsed_ms" => 5
      })

    bounded_result(binding, roots, providers)
    |> Map.merge(%{
      "policy" => "bounded_depth_v2",
      "status" => "error",
      "bounded_complete" => false,
      "details" => %{"kind" => "provider_header_limit"},
      "direct_checkpoint" => checkpoint
    })
  end

  test "natural254 caps deep coverage but complete checkpoint preserves all46 in1/45" do
    alias BorsNG.Worker.FailureDiagnostics

    data =
      "test/fixtures/provider-checkpoint-candidate254.json" |> File.read!() |> Jason.decode!()

    assert CandidateJournal.digest(data["members"]) == data["binding"]["members_digest"]
    assert length(data["members"]) == 46
    assert :unavailable = FailureDiagnostics.map_paths(data["hints"], data["logs"])

    assert :unavailable =
             FailureDiagnostics.map_retry_providers(
               data["hints"],
               data["logs"],
               data["binding"],
               data["recorded_lookup"]
             )

    lookup = data["provider_lookup"]
    assert lookup["bounded_complete"] == false
    assert lookup["full_closure_complete"] == false
    assert lookup["details"]["kind"] == "provider_header_limit"

    assert {:ok, [16848], "import_provider_hint"} =
             FailureDiagnostics.map_retry_providers(
               data["hints"],
               data["logs"],
               data["binding"],
               lookup
             )

    assert :unavailable =
             FailureDiagnostics.map_bounded_providers(
               data["hints"],
               data["logs"],
               data["binding"],
               lookup
             )

    links =
      Enum.map(data["members"], fn m ->
        %LinkPatchBatch{
          id: m["link_id"],
          patch_id: m["patch_id"],
          head_sha: m["head_sha"],
          reviewer: m["reviewer"],
          manual_approval: m["manual_approval"],
          patch: %Patch{
            pr_xref: m["pr"],
            commit: m["head_sha"],
            bundle_id: m["bundle_id"],
            is_single: get_in(m, ["scheduling", "singleton"]) == true
          }
        }
      end)

    assert :fallback = FailureIsolation.partition(links, data["hints"], nil)

    assert {:ok, groups, "import_provider_hint"} =
             FailureIsolation.partition(
               links,
               Map.put(data["hints"], "provider_suspects", [16848]),
               nil
             )

    assert Enum.map(groups, &length/1) == [1, 45]
    assert hd(prs(groups)) == [16848]

    snapshots =
      groups
      |> List.flatten()
      |> Enum.map(fn l -> Enum.find(data["members"], &(&1["link_id"] == l.id)) end)

    assert Enum.sort_by(snapshots, & &1["link_id"]) ==
             Enum.sort_by(data["members"], & &1["link_id"])

    assert data["recorded_baseline"]["id"] == 3775
    assert data["recorded_baseline"]["details"]["strategy"] == "deterministic_bisection"
  end

  test "checkpoint fallback requires separately complete bound all-root proof and combined limits",
       c do
    alias BorsNG.Worker.FailureDiagnostics
    logs = log_result(["TauCeti/Downstream.lean"])
    binding = %{"run_id" => 100}

    result =
      checkpoint_error(binding, ["TauCeti/Downstream.lean"], [
        "TauCeti/File2.lean",
        "TauCeti/File3.lean"
      ])

    assert {:ok, [2, 3], "import_provider_hint"} =
             FailureDiagnostics.map_retry_providers(c.hints, logs, binding, result)

    for bad <- [
          Map.put(result, "policy", "bounded_depth_v1"),
          Map.put(result, "policy", "unknown"),
          Map.put(result, "bounded_complete", true),
          Map.put(result, "complete", true),
          Map.put(result, "binding", %{}),
          Map.put(result, "roots", []),
          Map.put(result, "proof_authority", true),
          Map.put(result, "full_closure_complete", true),
          Map.delete(result, "status"),
          Map.delete(result, "direct_checkpoint"),
          Map.put(result, "details", %{}),
          Map.put(result, "padding", String.duplicate("x", 65536)),
          put_in(result, ["limits", "max_headers"], 65),
          put_in(result, ["direct_checkpoint", "complete"], false),
          put_in(result, ["direct_checkpoint", "policy"], "unknown"),
          put_in(result, ["direct_checkpoint", "binding"], %{}),
          put_in(result, ["direct_checkpoint", "files"], []),
          put_in(result, ["direct_checkpoint", "paths"], []),
          put_in(result, ["direct_checkpoint", "headers"], 0),
          put_in(result, ["direct_checkpoint", "source_bytes"], 524_289),
          put_in(result, ["direct_checkpoint", "completed_elapsed_ms"], 5001),
          put_in(result, ["direct_checkpoint", "files", Access.at(0), "base_blob"], "moved"),
          put_in(
            result,
            ["direct_checkpoint", "files", Access.at(0), "providers", Access.at(0), "path"],
            "../secret"
          ),
          put_in(
            result,
            ["direct_checkpoint", "files", Access.at(0), "providers", Access.at(0), "bytes"],
            262_145
          )
        ] do
      assert :unavailable = FailureDiagnostics.map_retry_providers(c.hints, logs, binding, bad)
    end

    roots = ["TauCeti/Downstream.lean", "TauCeti/Other.lean"]
    multi = checkpoint_error(binding, roots)

    assert {:ok, [3], "import_provider_hint"} =
             FailureDiagnostics.map_retry_providers(c.hints, log_result(roots), binding, multi)

    # A complete but ownerless second root cannot be silently ignored.
    multi = put_in(multi, ["direct_checkpoint", "files", Access.at(1), "providers"], [])

    assert :unavailable =
             FailureDiagnostics.map_retry_providers(c.hints, log_result(roots), binding, multi)

    # Existing bundle/component widening still applies to checkpoint suspects.
    [a, b, d, e] = c.links
    a = %{a | patch: %{a.patch | bundle_id: 7}}
    b = %{b | patch: %{b.patch | bundle_id: 7}}
    hints = c.hints |> Map.put("provider_suspects", [2]) |> Map.put("import_edges", [[2, 3]])

    assert {:ok, groups, "import_provider_hint"} =
             FailureIsolation.partition([a, b, d, e], hints, nil)

    assert prs(groups) == [[1, 2, 3], [4]]
  end

  test "bounded natural replays preserve all snapshots; candidate232 has no narrowing gain" do
    alias BorsNG.Worker.FailureDiagnostics

    for {id, expected, owner} <- [{230, [1, 40], 16291}, {232, [2, 35], 16625}] do
      data =
        "test/fixtures/bounded-providers-candidate#{id}.json" |> File.read!() |> Jason.decode!()

      assert CandidateJournal.digest(data["members"]) == data["binding"]["members_digest"]
      assert :unavailable = FailureDiagnostics.map_paths(data["hints"], data["logs"])

      assert {:ok, [^owner], "bounded_provider_hint"} =
               FailureDiagnostics.map_retry_providers(
                 data["hints"],
                 data["logs"],
                 data["binding"],
                 data["provider_lookup"]
               )

      # New v2 must preserve successful deep coverage rather than falling
      # back to an ownerless direct layer in these two real natural cases.
      old = data["provider_lookup"]
      graph = Map.new(old["nodes"], &{&1["path"], &1})

      files =
        Enum.map(old["roots"], fn path ->
          node = graph[path]

          Map.take(node, ["path", "blob", "base_blob", "bytes"])
          |> Map.put(
            "providers",
            Enum.map(node["providers"], fn provider ->
              Map.take(graph[provider], ["path", "blob", "bytes"])
            end)
          )
        end)

      checkpoint = %{
        "policy" => "direct_depth_v1",
        "complete" => true,
        "binding" => data["binding"],
        "authority" => "retry_only",
        "proof_authority" => false,
        "files" => files,
        "headers" => length(files),
        "source_bytes" => Enum.sum(Enum.map(files, & &1["bytes"])),
        "completed_elapsed_ms" => 10,
        "paths" =>
          (files ++ Enum.flat_map(files, & &1["providers"]))
          |> Enum.map(& &1["path"])
          |> Enum.uniq()
          |> Enum.sort()
      }

      updated =
        old |> Map.put("policy", "bounded_depth_v2") |> Map.put("direct_checkpoint", checkpoint)

      assert {:ok, [^owner], "bounded_provider_hint"} =
               FailureDiagnostics.map_retry_providers(
                 data["hints"],
                 data["logs"],
                 data["binding"],
                 updated
               )

      assert :unavailable =
               FailureDiagnostics.map_retry_providers(
                 data["hints"],
                 data["logs"],
                 data["binding"],
                 put_in(updated, ["direct_checkpoint", "files", Access.at(0), "bytes"], 0)
               )

      links =
        Enum.map(data["members"], fn m ->
          %LinkPatchBatch{
            id: m["link_id"],
            patch_id: m["patch_id"],
            head_sha: m["head_sha"],
            reviewer: m["reviewer"],
            manual_approval: m["manual_approval"],
            patch: %Patch{
              pr_xref: m["pr"],
              commit: m["head_sha"],
              bundle_id: m["bundle_id"],
              is_single: get_in(m, ["scheduling", "singleton"]) == true
            }
          }
        end)

      assert {:ok, groups, "bounded_provider_hint"} =
               FailureIsolation.partition(
                 links,
                 Map.put(data["hints"], "bounded_provider_suspects", [owner]),
                 nil
               )

      assert Enum.map(groups, &length/1) == expected

      snapshots =
        groups
        |> List.flatten()
        |> Enum.map(fn l -> Enum.find(data["members"], &(&1["link_id"] == l.id)) end)

      assert Enum.sort_by(snapshots, & &1["link_id"]) ==
               Enum.sort_by(data["members"], & &1["link_id"])

      if id == 232 do
        assert {:ok, baseline, "dependency_component"} =
                 FailureIsolation.partition(links, data["hints"], nil)

        assert prs(groups) == prs(baseline)
        assert prs(groups) == data["recorded_baseline"]["details"]["groups"]
      end
    end
  end

  for {kind, reason} <- [
        {:bounded, "bounded_provider_hint"},
        {:checkpoint, "import_provider_hint"}
      ] do
    test "one weak #{kind} owner cannot invoke provisional eviction or diagnosis", c do
      alias BorsNG.Worker.FailureDiagnostics
      {batch, evidence, hints} = provider_terminal(c)
      logs = log_result(["TauCeti/Downstream.lean"])

      lookup = fn _, :conn, binding, paths ->
        if unquote(kind) == :bounded,
          do: bounded_result(binding, paths),
          else: checkpoint_error(binding, paths)
      end

      assert :ok = FailureDiagnostics.observe(batch, evidence, :conn, fn _, _ -> logs end, lookup)
      FailureIsolation.classified(batch, "candidate_failure", evidence)
      assert :unavailable = FailureDiagnostics.diagnosis(c.candidate, hints)
      assert :unavailable = FailureDiagnostics.suspects(c.candidate, hints)

      assert {:ok, [3], unquote(reason)} =
               FailureDiagnostics.retry_suspects(c.candidate, hints)

      # No mocked GitHub call: an attempted provisional repair fails the test.
      assert :retrying = FailureIsolation.split(c.links, batch, nil, :conn)
      children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
      groups = Enum.map(children, &stored_links(&1.id))
      assert prs(groups) == [[3], [1, 2, 4]]
      assert Enum.all?(children, & &1.sealed)

      for link <- List.flatten(groups) do
        original = Enum.find(c.links, &(&1.patch_id == link.patch_id))

        assert Map.take(link, [:head_sha, :reviewer, :manual_approval]) ==
                 Map.take(original, [:head_sha, :reviewer, :manual_approval])

        admission = CandidateJournal.admission(link.id).snapshot
        before = CandidateJournal.admission(original.id)
        assert admission["original_admitted_at"] == before.snapshot["original_admitted_at"]
        assert admission["parent_candidate_id"] == c.candidate.id
        assert admission["parent_admission_id"] == before.id
        assert admission["retry_reason"] == unquote(reason)
        assert link.patch.queue_approval == original.patch.queue_approval
        assert link.patch.source_compatibility == original.patch.source_compatibility
        assert link.patch.bundle_id == original.patch.bundle_id
      end

      refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "provisional_repair"))

      assert :ok =
               FailureDiagnostics.observe(
                 batch,
                 evidence,
                 :conn,
                 fn _, _ -> flunk("immutable logs repeated") end,
                 fn _, _, _, _ -> flunk("immutable lookup repeated") end
               )

      FailureIsolation.classified(batch, "candidate_failure", %{evidence | run_attempt: 2})
      assert :unavailable = FailureDiagnostics.retry_suspects(c.candidate, hints)
    end
  end

  test "bounded validation rejects partial graphs, moved bindings, caps and forged coverage", c do
    alias BorsNG.Worker.FailureDiagnostics
    logs = log_result(["TauCeti/Downstream.lean"])
    binding = %{"run_id" => 100}

    result =
      bounded_result(binding, ["TauCeti/Downstream.lean"], [
        "TauCeti/File2.lean",
        "TauCeti/File3.lean"
      ])

    assert {:ok, [2, 3]} =
             FailureDiagnostics.map_bounded_providers(c.hints, logs, binding, result)

    for bad <- [
          Map.put(result, "bounded_complete", false),
          Map.put(result, "complete", true),
          Map.put(result, "full_closure_complete", true),
          Map.put(result, "binding", %{}),
          Map.put(result, "proof_authority", true),
          Map.put(result, "elapsed_ms", 5001),
          Map.put(result, "source_bytes", 0),
          Map.put(result, "headers", 65),
          Map.put(result, "nodes", Enum.take(result["nodes"], 2)),
          Map.put(result, "changed_frontier", []),
          Map.put(result, "residual_frontier", ["hidden"]),
          put_in(result, ["limits", "max_depth"], 6),
          put_in(result, ["nodes", Access.at(0), "base_blob"], "moved"),
          put_in(result, ["nodes", Access.at(0), "providers"], ["../secret"]),
          put_in(result, ["nodes", Access.at(1), "stop"], "depth_limit")
        ] do
      assert :unavailable = FailureDiagnostics.map_retry_providers(c.hints, logs, binding, bad)
    end

    # Every diagnostic root needs a reachable changed frontier, even when roots share paths.
    roots = ["TauCeti/Downstream.lean", "TauCeti/Other.lean"]
    multi = bounded_result(binding, roots)

    assert {:ok, [3]} =
             FailureDiagnostics.map_bounded_providers(c.hints, log_result(roots), binding, multi)

    multi = put_in(multi, ["nodes", Access.at(1), "providers"], [])

    assert :unavailable =
             FailureDiagnostics.map_bounded_providers(c.hints, log_result(roots), binding, multi)
  end

  test "exact candidate225 production replay narrows 1/21 and preserves every frozen snapshot" do
    alias BorsNG.Worker.FailureDiagnostics
    data = "test/fixtures/import-providers-candidate225.json" |> File.read!() |> Jason.decode!()
    assert CandidateJournal.digest(data["members"]) == data["binding"]["members_digest"]
    assert :unavailable = FailureDiagnostics.map_paths(data["hints"], data["logs"])

    assert {:ok, [16713]} =
             FailureDiagnostics.map_providers(
               data["hints"],
               data["logs"],
               data["binding"],
               data["provider_lookup"]
             )

    links =
      Enum.map(data["members"], fn m ->
        %LinkPatchBatch{
          id: m["link_id"],
          patch_id: m["patch_id"],
          head_sha: m["head_sha"],
          reviewer: m["reviewer"],
          manual_approval: m["manual_approval"],
          patch: %Patch{
            pr_xref: m["pr"],
            commit: m["head_sha"],
            bundle_id: m["bundle_id"],
            is_single: get_in(m, ["scheduling", "singleton"]) == true
          }
        }
      end)

    assert :fallback = FailureIsolation.partition(links, data["hints"], nil)

    assert {:ok, groups, "import_provider_hint"} =
             FailureIsolation.partition(
               links,
               Map.put(data["hints"], "provider_suspects", [16713]),
               nil
             )

    assert Enum.map(groups, &length/1) == [1, 21]
    assert prs(groups) |> hd() == [16713]
    original = Map.new(data["members"], &{&1["link_id"], &1})

    rebuilt =
      groups |> List.flatten() |> Enum.map(&original[&1.id]) |> Enum.sort_by(& &1["link_id"])

    assert rebuilt == Enum.sort_by(data["members"], & &1["link_id"])
    assert length(rebuilt) == 22
    assert data["recorded_baseline"]["id"] == 3121
    assert data["recorded_baseline"]["details"]["strategy"] == "deterministic_bisection"
    # The unchanged production divider bisects 22 atomic singleton units 11/11.
    assert links |> BorsNG.Worker.Batcher.Divider.group_units() |> length() == 22
  end

  test "provider hint schedules sealed retries and never authorizes provisional repair", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence, hints} = provider_terminal(c)
    result = log_result(["TauCeti/Downstream.lean"])
    lookup = fn _, :conn, binding, paths -> provider_result(binding, paths) end
    assert :ok = FailureDiagnostics.observe(batch, evidence, :conn, fn _, _ -> result end, lookup)
    FailureIsolation.classified(batch, "candidate_failure", evidence)
    assert :unavailable = FailureDiagnostics.diagnosis(c.candidate, hints)

    assert {:ok, [3], "import_provider_hint"} =
             FailureDiagnostics.retry_suspects(c.candidate, hints)

    # Any attempt to call the provisional GitHub repair interface fails this test.
    assert :retrying = FailureIsolation.split(c.links, batch, nil, :conn)
    children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
    groups = Enum.map(children, &stored_links(&1.id))
    assert prs(groups) == [[3], [1, 2, 4]]
    assert Enum.all?(children, & &1.sealed)
    original = Map.new(c.links, &{&1.patch_id, &1})

    for link <- List.flatten(groups) do
      before = original[link.patch_id]
      assert link.head_sha == before.head_sha
      assert link.reviewer == before.reviewer
      assert link.manual_approval == before.manual_approval
      admission = CandidateJournal.admission(link.id).snapshot
      original_admission = CandidateJournal.admission(before.id)

      assert admission["original_admitted_at"] ==
               original_admission.snapshot["original_admitted_at"]

      assert admission["parent_candidate_id"] == c.candidate.id
      assert admission["parent_admission_id"] == original_admission.id
      assert admission["retry_reason"] == "import_provider_hint"
      assert link.patch.queue_pending
      assert link.patch.queue_approval == before.patch.queue_approval
      assert link.patch.source_compatibility == before.patch.source_compatibility
    end

    refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "provisional_repair"))

    assert :ok =
             FailureDiagnostics.observe(
               batch,
               evidence,
               :conn,
               fn _, _ -> flunk("logs repeated") end,
               fn _, _, _, _ -> flunk("lookup repeated") end
             )

    assert :retrying = FailureIsolation.split(c.links, batch, nil, :conn)
    assert Repo.aggregate(Batch, :count, :id) == 3
  end

  test "provider mapping requires complete bounded paths and retains all changed providers", c do
    alias BorsNG.Worker.FailureDiagnostics
    logs = log_result(["TauCeti/Downstream.lean"])
    bound = %{"run_id" => 100}

    result =
      provider_result(bound, ["TauCeti/Downstream.lean"], [
        "TauCeti/File2.lean",
        "TauCeti/File3.lean",
        "TauCeti/Unrelated.lean"
      ])

    assert {:ok, [2, 3]} = FailureDiagnostics.map_providers(c.hints, logs, bound, result)
    assert :unavailable = FailureDiagnostics.map_paths(c.hints, logs)

    for bad <- [
          Map.put(result, "complete", false),
          Map.put(result, "binding", %{}),
          Map.put(result, "files", []),
          Map.put(result, "proof_authority", true),
          put_in(result, ["files", Access.at(0), "blob"], "--option"),
          put_in(result, ["files", Access.at(0), "bytes"], 262_145),
          put_in(result, ["files", Access.at(0), "providers", Access.at(0), "path"], "../secret"),
          provider_result(bound, ["TauCeti/Downstream.lean"], ["TauCeti/Unrelated.lean"])
        ] do
      assert :unavailable = FailureDiagnostics.map_providers(c.hints, logs, bound, bad)
    end

    for bad_logs <- [
          log_result(["../secret"]),
          log_result(["TauCeti/../secret.lean"]),
          log_result(["TauCeti/A.lean\n"]),
          log_result(["TauCeti/File3.lean"]),
          log_result(["TauCeti/Downstream.lean", "TauCeti/File3.lean"]),
          log_result(Enum.map(1..9, &"TauCeti/Downstream#{&1}.lean")),
          log_result(["TauCeti/Downstream.lean"], "lake_failed_target"),
          log_result([], "environment_linter")
        ] do
      assert :unavailable = FailureDiagnostics.provider_paths(c.hints, bad_logs)
    end
  end

  test "provider planner widens components and bundles, preserves manual approvals and falls back",
       c do
    [a, b, d, e] = c.links
    a = %{a | manual_approval: %{"reviewer" => "human"}, patch: %{a.patch | bundle_id: 7}}
    b = %{b | patch: %{b.patch | bundle_id: 7}}
    hints = c.hints |> Map.put("provider_suspects", [2]) |> Map.put("import_edges", [[2, 3]])

    assert {:ok, groups, "import_provider_hint"} =
             FailureIsolation.partition([a, b, d, e], hints, nil)

    assert prs(groups) == [[1, 2, 3], [4]]
    assert hd(hd(groups)).manual_approval == %{"reviewer" => "human"}

    assert :fallback =
             FailureIsolation.partition(
               [a, b, d, e],
               Map.put(hints, "import_edges", [[2, 3], [3, 4]]),
               nil
             )

    assert FailureIsolation.partition(
             c.links,
             Map.put(hints, "provider_suspects", [1, 2, 3, 4]),
             nil
           ) ==
             FailureIsolation.partition(c.links, Map.delete(hints, "provider_suspects"), nil)

    assert :fallback =
             FailureIsolation.partition(c.links, Map.put(hints, "ownership_complete", false), nil)
  end

  test "provider lookup rejects stale binding or ownership before reading and persists unavailable",
       c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence, hints} = provider_terminal(c)
    logs = log_result(["TauCeti/Downstream.lean"])
    reject = fn _, _, _, _ -> flunk("ineligible lookup read Git") end
    bad_batch = %{batch | base_sha: "moved"}
    bad_evidence = %{evidence | identity: "wrong"}
    FailureDiagnostics.observe(batch, bad_evidence, :conn, fn _, _ -> logs end, reject)
    moved = %{evidence | identity: "bors branch=main batch=#{batch.id} head=staged base=moved"}
    FailureDiagnostics.observe(bad_batch, moved, :conn, fn _, _ -> logs end, reject)
    FailureIsolation.classified(batch, "candidate_failure", evidence)
    assert :unavailable = FailureDiagnostics.retry_suspects(c.candidate, hints)
    # Unavailable is final for this run/attempt, even once optional coverage recovers.
    FailureDiagnostics.observe(
      batch,
      evidence,
      :conn,
      fn _, _ -> flunk("logs repeated") end,
      reject
    )

    for moved <- [
          %{c.candidate | generation: 2},
          %{c.candidate | snapshot: Map.put(c.candidate.snapshot, "base_sha", "other")}
        ] do
      assert :unavailable = FailureDiagnostics.retry_suspects(moved, hints)
    end

    for newer <- [%{evidence | run_id: 101}, %{evidence | run_attempt: 2}] do
      FailureIsolation.classified(batch, "candidate_failure", newer)
      assert :unavailable = FailureDiagnostics.retry_suspects(c.candidate, hints)
    end
  end

  test "provider lookup does not read when a frozen member head has moved", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence, _} = provider_terminal(c)
    hd(c.links).patch |> Patch.changeset(%{commit: "new-head"}) |> Repo.update!()

    FailureDiagnostics.observe(
      batch,
      evidence,
      :conn,
      fn _, _ -> log_result(["TauCeti/Downstream.lean"]) end,
      fn _, _, _, _ -> flunk("moved head read Git") end
    )
  end

  test "cascading target overflow preserves precise retry hints but not global unknowns", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence} = terminal(c)
    primary = "error: TauCeti/File3.lean:320:0: section variable mismatch\n"

    cascade =
      "Some required targets logged failures:\n" <>
        Enum.map_join(1..1400, "\n", &"- TauCeti.Cascade#{&1}") <>
        "\nerror: build failed\n"

    result = parsed_log_result(primary <> cascade)
    assert {:ok, [3]} = FailureDiagnostics.map_paths(c.hints, result)

    unknown = %{
      version: 1,
      complete: true,
      findings: [%{linter: "simpNF", declaration: "_private.Probe", unknown: "missing_range"}]
    }

    assert :unavailable =
             FailureDiagnostics.map_paths(
               c.hints,
               parsed_log_result(
                 primary <> cascade <> "LINT-ENV-LOCATIONS " <> Jason.encode!(unknown)
               )
             )

    assert :ok = FailureDiagnostics.observe(batch, evidence, :conn, fn _, [200] -> result end)
    FailureIsolation.classified(batch, "candidate_failure", %{evidence | run_attempt: 2})
    assert :unavailable = FailureDiagnostics.suspects(c.candidate, c.hints)
    FailureIsolation.classified(batch, "candidate_failure", evidence)
    assert :retrying = FailureIsolation.split(c.links, batch)
    children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
    groups = Enum.map(children, &stored_links(&1.id))
    assert prs(groups) == [[3], [1, 2, 4]]
    assert Enum.all?(children, &(&1.sealed and &1.state == :waiting))

    assert Enum.sort(Enum.map(List.flatten(groups), &{&1.patch_id, &1.head_sha, &1.reviewer})) ==
             Enum.sort(Enum.map(c.links, &{&1.patch_id, &1.head_sha, &1.reviewer}))

    assert Enum.all?(Repo.all(Patch), &(&1.open and &1.queue_approval["state"] == "approved"))
  end

  test "iofail failed-target hints retain authoritative run binding and full retry checks", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence} = terminal(c)

    result =
      parsed_log_result("""
      warning: TauCeti/Unrelated.lean:9:0: deprecated import
      Some required targets logged failures:
      - TauCeti.File2
      - TauCeti.File3
      error: build failed
      """)

    assert {:ok, [2, 3]} = FailureDiagnostics.map_paths(c.hints, result)
    assert :ok = FailureDiagnostics.observe(batch, evidence, :conn, fn _, [200] -> result end)
    FailureIsolation.classified(batch, "candidate_failure", %{evidence | run_id: 101})
    assert :unavailable = FailureDiagnostics.suspects(c.candidate, c.hints)
    FailureIsolation.classified(batch, "candidate_failure", evidence)
    assert :retrying = FailureIsolation.split(c.links, batch)
    children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
    assert prs(Enum.map(children, &stored_links(&1.id))) == [[2, 3], [1, 4]]
    assert Enum.all?(children, &(&1.sealed and &1.state == :waiting))
    assert Enum.all?(Repo.all(Patch), &(&1.open and &1.queue_approval["state"] == "approved"))
  end

  test "environment origins only schedule fully checked immutable retries", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence} = terminal(c)

    finding = %{
      linter: "simpNF",
      declaration: "TauCeti.λ雪",
      path: "TauCeti/File3.lean",
      line: 77,
      column: 9
    }

    result = environment_result([finding])
    assert :ok = FailureDiagnostics.observe(batch, evidence, :conn, fn _, [200] -> result end)
    FailureIsolation.classified(batch, "candidate_failure", evidence)
    assert {:ok, [3]} = FailureDiagnostics.suspects(c.candidate, c.hints)

    for changed <- [
          %{c.candidate | generation: c.candidate.generation + 1},
          %{c.candidate | snapshot: Map.put(c.candidate.snapshot, "base_sha", "other")},
          %{c.candidate | snapshot: Map.update!(c.candidate.snapshot, "members", &Enum.reverse/1)}
        ] do
      assert :unavailable = FailureDiagnostics.suspects(changed, c.hints)
    end

    FailureIsolation.classified(batch, "candidate_failure", %{evidence | run_attempt: 2})
    assert :unavailable = FailureDiagnostics.suspects(c.candidate, c.hints)
    FailureIsolation.classified(batch, "candidate_failure", evidence)
    assert :retrying = FailureIsolation.split(c.links, batch)
    children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
    assert prs(Enum.map(children, &stored_links(&1.id))) == [[3], [1, 2, 4]]
    assert Enum.all?(children, &(&1.sealed and &1.state == :waiting))
    assert Enum.all?(Repo.all(Patch), &(&1.open and &1.queue_approval["state"] == "approved"))
    [event] = Repo.all(from(e in CandidateEvent, where: e.kind == "failure_diagnostics"))
    assert event.details["authority"] == "retry_only"
    refute event.details["proof_authority"]
  end

  test "global and unknown environment findings retain every owner or fall back", c do
    alias BorsNG.Worker.FailureDiagnostics

    finding = %{
      linter: "simpNF",
      declaration: "TauCeti.Probe",
      path: "TauCeti/File2.lean",
      line: 1,
      column: 1
    }

    other = %{finding | path: "TauCeti/File3.lean", declaration: "TauCeti.Other"}

    assert {:ok, [2, 3]} =
             FailureDiagnostics.map_paths(c.hints, environment_result([finding, other]))

    unknown = %{linter: "checkType", declaration: "_private.Probe", unknown: "missing_range"}

    assert :unavailable =
             FailureDiagnostics.map_paths(c.hints, environment_result([finding, unknown]))

    unchanged = %{finding | path: "TauCeti/Provider.lean"}
    assert :unavailable = FailureDiagnostics.map_paths(c.hints, environment_result([unchanged]))

    [first | rest] = c.hints["changed_files"]

    hints =
      Map.put(c.hints, "changed_files", [
        Map.put(first, "dependency_paths", ["TauCeti/Provider.lean"]) | rest
      ])

    assert {:ok, [1]} = FailureDiagnostics.map_paths(hints, environment_result([unchanged]))
  end

  defp ownership_hints(c) do
    provenance =
      BorsNG.Worker.CandidateAssembler.identity(c.candidate)
      |> Jason.encode!()
      |> Jason.decode!()
      |> Map.merge(%{
        "commit" => "staged",
        "tree" => "tree",
        "members" => Enum.map(c.candidate.snapshot["members"], &Map.take(&1, ["pr", "head_sha"]))
      })

    c.hints
    |> Map.merge(%{
      "complete" => false,
      "ownership_complete" => true,
      "dependencies_complete" => false,
      "reason" => "timeout",
      "provenance" => provenance
    })
  end

  test "optional dependency timeout retains compiler isolation and immutable approvals", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence} = terminal(c)
    hints = ownership_hints(c)

    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", %{
      commit: "staged",
      tree: "tree",
      evidence: %{diagnostic_hints: hints}
    })

    assert FailureIsolation.hints(c.candidate) == hints

    assert :ok =
             FailureDiagnostics.observe(batch, evidence, :conn, fn _, [200] ->
               log_result(["TauCeti/File3.lean"])
             end)

    FailureIsolation.classified(batch, "candidate_failure", evidence)
    assert :retrying = FailureIsolation.split(Enum.reverse(c.links), batch)
    children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
    groups = Enum.map(children, &stored_links(&1.id))
    assert prs(groups) == [[3], [1, 2, 4]]

    assert Enum.sort(
             Enum.map(
               List.flatten(groups),
               &{&1.patch_id, &1.head_sha, &1.reviewer, &1.manual_approval}
             )
           ) ==
             Enum.sort(
               Enum.map(c.links, &{&1.patch_id, &1.head_sha, &1.reviewer, &1.manual_approval})
             )

    assert Enum.all?(Repo.all(Patch), &(&1.queue_approval["state"] == "approved"))
    [event] = Repo.all(from(e in CandidateEvent, where: e.kind == "failure_partition"))
    assert event.details["strategy"] == "compiler_diagnostics"
    assert event.details["ownership_complete"]
    refute event.details["dependencies_complete"]
    assert event.details["authority"] == "retry_only"
  end

  test "ownership-only evidence cannot use unknown imports or guess unchanged paths", c do
    alias BorsNG.Worker.FailureDiagnostics

    hints =
      ownership_hints(c)
      |> Map.put("missing_imports", [%{"pr" => 1}])
      |> Map.put("import_edges", [[1, 2]])

    assert :fallback = FailureIsolation.partition(c.links, hints, nil)

    assert :unavailable =
             FailureDiagnostics.map_paths(hints, log_result(["TauCeti/Provider.lean"]))

    assert {:ok, [3]} = FailureDiagnostics.map_paths(hints, log_result(["TauCeti/File3.lean"]))

    assert :unavailable =
             FailureDiagnostics.map_paths(
               hints,
               log_result(["TauCeti/File3.lean", "TauCeti/Unknown.lean"])
             )

    # Every possible owner of a shared path stays in the retry; unknown imports
    # cannot create additional ownership or preflight missing-source verdicts.
    hints = put_in(hints, ["changed_files", Access.at(1), "paths"], ["TauCeti/File3.lean"])
    assert :fallback = FailureIsolation.partition(c.links, hints, nil)
    assert {:ok, [2, 3]} = FailureDiagnostics.map_paths(hints, log_result(["TauCeti/File3.lean"]))

    assert {:ok, groups, "compiler_diagnostics"} =
             FailureIsolation.partition(c.links, Map.put(hints, "compiler_suspects", [2, 3]), nil)

    assert prs(groups) == [[2, 3], [1, 4]]
  end

  test "ownership provenance rejects another base tree commit order or exact head", c do
    hints =
      ownership_hints(c)
      |> Map.merge(%{
        "complete" => true,
        "dependencies_complete" => true,
        "missing_imports" => [%{"pr" => 3}]
      })

    for broken <- [
          put_in(hints, ["provenance", "base_sha"], "other"),
          put_in(hints, ["provenance", "tree"], "other"),
          put_in(hints, ["provenance", "commit"], "other"),
          update_in(hints, ["provenance", "members"], &Enum.reverse/1),
          put_in(hints, ["changed_files", Access.at(0), "head_sha"], "other")
        ] do
      CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", %{
        commit: "staged",
        tree: "tree",
        evidence: %{diagnostic_hints: broken}
      })

      assert FailureIsolation.hints(c.candidate) == nil
      # All other evidence is complete: provenance alone must block preflight.
      assert :dispatch =
               FailureIsolation.preflight(c.links, c.batch, %{
                 commit: "staged",
                 tree: "tree",
                 evidence: %{"diagnostic_hints" => broken}
               })
    end
  end

  test "complete new ownership provenance permits an exact missing-import retry", c do
    hints =
      ownership_hints(c)
      |> Map.merge(%{
        "complete" => true,
        "dependencies_complete" => true,
        "missing_imports" => [%{"pr" => 3}]
      })

    result = %{commit: "staged", tree: "tree", evidence: %{"diagnostic_hints" => hints}}
    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", result)
    assert :partitioned = FailureIsolation.preflight(c.links, c.batch, result)
    children = Repo.all(from(b in Batch, where: b.id != ^c.batch.id, order_by: b.id))
    assert prs(Enum.map(children, &stored_links(&1.id))) == [[3], [1, 2, 4]]
  end

  test "terminal diagnostics split exact membership with approvals and survive restart", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence} = terminal(c)
    collector = fn _, [200] -> log_result(["TauCeti/File3.lean"]) end
    assert :ok = FailureDiagnostics.observe(batch, evidence, :conn, collector)
    FailureIsolation.classified(batch, "candidate_failure", evidence)
    assert :retrying = FailureIsolation.split(Enum.reverse(c.links), batch)
    children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
    assert prs(Enum.map(children, &stored_links(&1.id))) == [[3], [1, 2, 4]]
    assert Enum.all?(children, & &1.sealed)

    assert Enum.all?(
             Repo.all(Patch),
             &(&1.queue_pending and &1.queue_approval["state"] == "approved")
           )

    assert :ok =
             FailureDiagnostics.observe(batch, evidence, :conn, fn _, _ ->
               flunk("logs re-read")
             end)

    assert :retrying = FailureIsolation.split(c.links, batch)
    assert Repo.aggregate(Batch, :count, :id) == 3
    [event] = Repo.all(from(e in CandidateEvent, where: e.kind == "failure_diagnostics"))
    assert event.details["binding"]["candidate_id"] == c.candidate.id
    assert event.details["binding"]["generation"] == c.candidate.generation
    assert event.details["binding"]["job_ids"] == [200]
    assert event.details["authority"] == "retry_only"
    refute event.details["proof_authority"]

    # A newer authoritative observation for a different job cannot replay the
    # older job's compiler output, and infrastructure cannot consume it at all.
    other = put_in(evidence, [:failed_steps, Access.at(0), :job_id], 201)
    FailureIsolation.classified(batch, "candidate_failure", other)
    assert :unavailable = FailureDiagnostics.suspects(c.candidate, c.hints)
    FailureIsolation.classified(batch, "infrastructure", evidence)
    assert :unavailable = FailureDiagnostics.suspects(c.candidate, c.hints)
  end

  test "unavailable diagnostics are durable and stale runs/heads cannot supply hints", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence} = terminal(c)
    reject = fn _, _ -> flunk("stale/infrastructure evidence fetched") end
    FailureDiagnostics.observe(%{batch | commit: "old"}, evidence, :conn, reject)

    FailureDiagnostics.observe(
      batch,
      %{evidence | run_created_at: "2000-01-01T00:00:00Z"},
      :conn,
      reject
    )

    FailureDiagnostics.observe(batch, %{evidence | conclusion: "cancelled"}, :conn, reject)
    FailureDiagnostics.observe(batch, %{evidence | failed_steps: []}, :conn, reject)
    # The unavailable observation for no authoritative failed job is durable.
    assert :unavailable = FailureDiagnostics.suspects(c.candidate, c.hints)
    evidence = %{evidence | run_attempt: 2}
    FailureDiagnostics.observe(batch, evidence, :conn, fn _, _ -> %{"complete" => false} end)
    FailureIsolation.classified(batch, "candidate_failure", evidence)

    FailureDiagnostics.observe(batch, evidence, :conn, fn _, _ ->
      flunk("unavailable result re-read")
    end)

    assert :unavailable = FailureDiagnostics.suspects(c.candidate, c.hints)
    FailureIsolation.classified(batch, "candidate_failure", %{evidence | run_attempt: 3})
    assert :unavailable = FailureDiagnostics.suspects(c.candidate, c.hints)
  end

  test "compiler and audit suspects preserve bundles, provider groups and every offender", c do
    [a, b, d, e] = c.links
    a = %{a | patch: %{a.patch | bundle_id: 7}}
    b = %{b | patch: %{b.patch | bundle_id: 7}}
    hints = c.hints |> Map.put("compiler_suspects", [2]) |> Map.put("import_edges", [[2, 3]])

    assert {:ok, groups, "compiler_diagnostics"} =
             FailureIsolation.partition([a, b, d, e], hints, nil)

    assert prs(groups) == [[1, 2, 3], [4]]
    hints = c.hints |> Map.put("compiler_suspects", [1, 3])
    assert {:ok, groups, "compiler_diagnostics"} = FailureIsolation.partition(c.links, hints, nil)
    assert prs(groups) == [[1, 3], [2, 4]]

    assert :fallback =
             FailureIsolation.partition(
               c.links,
               Map.put(hints, "compiler_suspects", [1, 2, 3, 4]),
               nil
             )
  end

  test "unchanged dependency errors require attributed imports; audit retains both sides", c do
    alias BorsNG.Worker.FailureDiagnostics

    assert {:ok, [2, 3]} =
             FailureDiagnostics.map_paths(
               c.hints,
               log_result(["TauCeti/File2.lean", "TauCeti/File3.lean"], "duplicate_declaration")
             )

    assert {:ok, [2]} =
             FailureDiagnostics.map_paths(
               c.hints,
               log_result(["TauCeti/File2.lean", "TauCeti/Base.lean"], "duplicate_declaration")
             )

    assert :unavailable = FailureDiagnostics.map_paths(c.hints, log_result(["TauCeti/Base.lean"]))

    assert :unavailable =
             FailureDiagnostics.map_paths(c.hints, log_result(["TauCeti/Provider.lean"]))

    [first | rest] = c.hints["changed_files"]

    hints =
      Map.put(c.hints, "changed_files", [
        Map.put(first, "dependency_paths", ["TauCeti/Provider.lean"]) | rest
      ])

    assert {:ok, [1]} = FailureDiagnostics.map_paths(hints, log_result(["TauCeti/Provider.lean"]))
    assert :unavailable = FailureDiagnostics.map_paths(c.hints, %{"complete" => false})

    assert :unavailable =
             FailureDiagnostics.map_paths(c.hints, %{"complete" => true, "jobs" => []})
  end

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

  test "exact assembled missing import skips dispatch only for a strict retry partition", c do
    hints =
      Map.put(c.hints, "missing_imports", [
        %{"pr" => 3, "path" => "TauCeti/File3.lean", "provider_path" => "TauCeti/Absent.lean"}
      ])

    result = %{commit: "assembled", tree: "tree", evidence: %{"diagnostic_hints" => hints}}
    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", result)
    assert :partitioned = FailureIsolation.preflight(c.links, c.batch, result)
    # A crash in the caller after this return must not leave a waiting parent
    # that can reconstruct against a changed main beside its live retry children.
    assert Repo.get!(Batch, c.batch.id).state == :error
    children = Repo.all(from(b in Batch, where: b.id != ^c.batch.id, order_by: b.id))
    assert prs(Enum.map(children, &stored_links(&1.id))) == [[3], [1, 2, 4]]

    assert Enum.all?(
             Repo.all(Patch),
             &(&1.queue_pending and &1.queue_approval["state"] == "approved")
           )

    assert :partitioned = FailureIsolation.preflight(c.links, c.batch, result)
    assert Repo.aggregate(Batch, :count, :id) == 3
    assert :dispatch = FailureIsolation.preflight([Enum.at(c.links, 2)], c.batch, result)
    assert :dispatch = FailureIsolation.preflight(c.links, c.batch, %{result | evidence: %{}})

    assert :dispatch =
             FailureIsolation.preflight(
               c.links,
               c.batch,
               put_in(result, [:evidence, "diagnostic_hints", "complete"], false)
             )

    assert Repo.all(from(e in CandidateEvent, where: e.kind == "head_quarantined")) == []
  end

  test "missing-import evidence, all retry children and parent retirement commit together", c do
    [first, second | _] = c.links
    bundle = Repo.insert!(PatchBundle.new(c.project.id))

    for link <- [first, second] do
      link.patch |> Patch.changeset(%{bundle_id: bundle.id}) |> Repo.update!()
    end

    links = stored_links(c.batch.id)

    # An existing partial bundle makes the second partition unavailable. The
    # first suspect child must roll back with its evidence, preserving the
    # waiting parent and the pre-existing membership for ordinary recovery.
    existing =
      BorsNG.Worker.Batcher.Divider.clone_batch(
        [hd(links)],
        c.project.id,
        "main",
        "existing_partial_bundle"
      )

    hints = Map.put(c.hints, "missing_imports", [%{"pr" => 3}])
    result = %{commit: "assembled", tree: "tree", evidence: %{"diagnostic_hints" => hints}}
    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", result)
    before_admissions = Repo.aggregate(BorsNG.Database.CandidateAdmission, :count, :id)

    assert catch_throw(FailureIsolation.preflight(links, c.batch, result)) ==
             :retry_membership_unavailable

    assert Repo.get!(Batch, c.batch.id).state == :waiting
    assert Repo.aggregate(Batch, :count, :id) == 2
    assert Enum.map(stored_links(existing.id), & &1.patch.pr_xref) == [1]
    assert Repo.aggregate(BorsNG.Database.CandidateAdmission, :count, :id) == before_admissions

    assert Repo.all(
             from(e in CandidateEvent,
               where: e.kind in ["local_import_failure", "failure_partition", "head_quarantined"]
             )
           ) == []

    assert Enum.all?(
             Repo.all(Patch),
             &(&1.queue_pending and &1.queue_approval["state"] == "approved")
           )
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

  describe "provisional diagnostic repair" do
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

      :ok
    end

    test "one bound owner skips singleton CI, preserves remainder and deduplicates restart", c do
      {batch, conn} = provisional_candidate(c)
      assert BorsNG.Worker.SpeculativeTrain.tracked?(c.candidate)
      before = Map.new(c.links, &{&1.patch_id, CandidateJournal.admission(&1.id).snapshot})
      assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
      assert Repo.get!(Batch, batch.id).state == :error
      refute BorsNG.Worker.SpeculativeTrain.event?(c.candidate, "train_released")
      children = Repo.all(from(b in Batch, where: b.id != ^batch.id))
      assert [child] = children
      assert child.sealed
      remaining = stored_links(child.id)
      assert prs([remaining]) == [[1, 2, 4]]

      for link <- remaining do
        admission = CandidateJournal.admission(link.id).snapshot
        assert admission["original_admitted_at"] == before[link.patch_id]["original_admitted_at"]
        assert admission["scheduling"] == before[link.patch_id]["scheduling"]
        assert admission["parent_candidate_id"] == c.candidate.id
        assert link.reviewer == "r"
        assert link.manual_approval == nil
        assert Repo.get!(Patch, link.patch_id).queue_approval["state"] == "approved"
      end

      suspect = Repo.get!(Patch, Enum.at(c.links, 2).patch_id)
      assert suspect.queue_approval["state"] == "withdrawn"
      assert suspect.source_compatibility["repair_policy"] == "provisional-diagnostic-v1"
      refute suspect.source_compatibility["provisional"]["proof_authority"]
      assert suspect.source_compatibility["provisional"]["binding"]["run_attempt"] == 1
      assert [%{payload: payload}] = Repo.all(BorsNG.Database.OrchestrationJob)
      {_, 3, body, marker} = :erlang.binary_to_term(payload, [:safe])
      assert body =~ "PROVISIONAL SUSPECT, not a proven individual failure"
      assert body =~ "candidate #{c.candidate.id}"
      assert body =~ "run attempt 1"
      assert marker == "<!--tauceti-rebase:v1 head3-->"
      assert Repo.aggregate(BorsNG.Database.Status, :count, :id) == 0
      assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
      assert Repo.aggregate(Batch, :count, :id) == 2
      assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 1
    end

    test "verified base collision uses existing atomic provisional handoff and keeps its neighbor",
         c do
      {batch, _, hints, _} = base_collision_terminal(c)
      conn = provisional_mock(c)
      assert {:ok, 3, _} = BorsNG.Worker.FailureDiagnostics.base_collision(c.candidate, hints)
      before = Map.new(c.links, &{&1.patch_id, CandidateJournal.admission(&1.id).snapshot})
      assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
      assert Repo.get!(Batch, batch.id).state == :error
      [child] = Repo.all(from(b in Batch, where: b.id != ^batch.id))
      remaining = stored_links(child.id)
      assert prs([remaining]) == [[1, 2, 4]]

      for link <- remaining do
        a = CandidateJournal.admission(link.id).snapshot

        assert Map.take(
                 a,
                 ~w(head_sha reviewer manual_approval original_admitted_at bundle_id scheduling)
               ) ==
                 Map.take(
                   before[link.patch_id],
                   ~w(head_sha reviewer manual_approval original_admitted_at bundle_id scheduling)
                 )
      end

      suspect = Repo.get!(Patch, Enum.at(c.links, 2).patch_id)
      assert suspect.queue_approval["state"] == "withdrawn"
      assert suspect.source_compatibility["repair_policy"] == "provisional-diagnostic-v1"
      refute suspect.source_compatibility["provisional"]["proof_authority"]
      [event] = Repo.all(from(e in CandidateEvent, where: e.kind == "provisional_repair"))
      assert event.details["pr"] == 3
      refute event.details["singleton_confirmation_dispatched"]
      [job] = Repo.all(BorsNG.Database.OrchestrationJob)
      {_, 3, body, _} = :erlang.binary_to_term(job.payload, [:safe])
      assert body =~ "PROVISIONAL SUSPECT, not a proven individual failure"
      assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
      assert Repo.aggregate(Batch, :count, :id) == 2
      assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 1
    end

    for variant <- [:manual, :held, :moved_main, :successor, :existing_request, :automatic_bump] do
      @collision_guard variant
      test "verified base collision respects existing #{@collision_guard} provisional guard", c do
        {batch, _, _, _} = base_collision_terminal(c)
        conn = provisional_mock(c)
        state = GitHub.ServerMock.get_state()
        patch = Enum.at(c.links, 2).patch

        case @collision_guard do
          :automatic_bump ->
            author =
              Repo.insert!(%BorsNG.Database.User{
                login: "tauceti-review-bot[bot]",
                user_xref: 17106
              })

            patch
            |> Ecto.Changeset.change(author_id: author.id, head_ref: "bump-mathlib/test")
            |> Repo.update!()

          :manual ->
            patch
            |> Ecto.Changeset.change(manual_approval: %{"head_sha" => "head3"})
            |> Repo.update!()

          :held ->
            GitHub.ServerMock.put_state(put_in(state, [conn, :labels, 3], ["keep"]))

          :moved_main ->
            GitHub.ServerMock.put_state(put_in(state, [conn, :branches, "main"], "new-main"))

          :successor ->
            CandidateJournal.event(c.candidate, "successor_reserved", "bors.train", %{
              candidate_id: 999
            })

          :existing_request ->
            patch
            |> Ecto.Changeset.change(
              source_compatibility: %{
                "repair_head" => "head3",
                "repair_policy" => "provisional-diagnostic-v1"
              }
            )
            |> Repo.update!()
        end

        assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
        refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "provisional_repair"))
        assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 0
        children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
        # The manual fixture changes only Patch, leaving its queued approval
        # unchanged. That now invalidates candidate membership, so historical
        # diagnostics also fail closed to ordinary deterministic bisection.
        expected = if @collision_guard == :manual, do: [[2, 3], [1, 4]], else: [[3], [1, 2, 4]]
        assert prs(Enum.map(children, &stored_links(&1.id))) == expected
        assert Enum.all?(Repo.all(Patch), & &1.queue_pending)
      end
    end

    test "innocent exact-head human approval survives provisional retirement", c do
      approval = %{"head_sha" => "head5", "reviewer" => "human", "scope" => "scripts"}

      innocent =
        Repo.insert!(%Patch{
          project_id: c.project.id,
          pr_xref: 5,
          commit: "head5",
          into_branch: "main",
          manual_approval: approval
        })

      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          batch_id: c.batch.id,
          patch_id: innocent.id,
          head_sha: "head5",
          manual_approval: approval,
          reviewer: "human"
        })
      )

      links = stored_links(c.batch.id)
      candidate = CandidateJournal.begin(c.batch, links, "base")

      hints =
        Map.update!(
          c.hints,
          "changed_files",
          &(&1 ++ [%{"pr" => 5, "head_sha" => "head5", "paths" => ["Scripts/helper.py"]}])
        )

      c = %{c | links: links, candidate: candidate, hints: hints}
      {batch, conn} = provisional_candidate(c)
      assert :retrying = FailureIsolation.split(links, batch, nil, conn)
      [child] = Repo.all(from(b in Batch, where: b.id != ^batch.id))
      preserved = stored_links(child.id) |> Enum.find(&(&1.patch_id == innocent.id))
      assert preserved.manual_approval == approval
      assert preserved.reviewer == "human"
      assert Repo.get!(Patch, innocent.id).manual_approval == approval

      assert CandidateJournal.admission(preserved.id).snapshot["parent_candidate_id"] ==
               candidate.id
    end

    test "parent moving after live reads rolls back all provisional children and notice", c do
      {batch, conn} = provisional_candidate(c)
      Repo.get!(Batch, batch.id) |> Batch.changeset(%{commit: "replacement"}) |> Repo.update!()
      suspect = Enum.at(c.links, 2)
      remainder = [Enum.reject(c.links, &(&1.id == suspect.id))]

      assert :unavailable =
               BorsNG.Worker.SourceCompatibility.provisional_failure(
                 batch,
                 c.links,
                 suspect,
                 remainder,
                 ownership_hints(c),
                 conn
               )

      assert Repo.aggregate(Batch, :count, :id) == 1
      assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 0
      assert Repo.get!(Patch, suspect.patch_id).queue_approval["state"] == "approved"
    end

    test "a genuine speculative predecessor keeps conservative retries", c do
      CandidateJournal.bind(
        c.candidate,
        "base",
        "parent-tree",
        "base",
        %BorsToml{status: ["build"]},
        %{}
      )

      successor = CandidateJournal.begin(c.batch, c.links, "base", c.candidate.id)
      c = %{c | candidate: successor}
      {batch, conn} = provisional_candidate(c)
      assert successor.snapshot["speculative_predecessor_id"] != nil
      assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
      assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 0
      refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "provisional_repair"))
      refute BorsNG.Worker.SpeculativeTrain.event?(successor, "train_released")
    end

    for variant <- [
          :multiple,
          :unowned,
          :generic,
          :wrong_attempt,
          :wrong_head,
          :moved_main,
          :held,
          :manual,
          :api_outage,
          :successor,
          :component
        ] do
      @variant variant
      test "#{variant} retains conservative fully checked retries", c do
        paths =
          case @variant do
            :multiple -> ["TauCeti/File2.lean", "TauCeti/File3.lean"]
            :unowned -> ["TauCeti/Unknown.lean"]
            _ -> ["TauCeti/File3.lean"]
          end

        {batch, conn} = provisional_candidate(c, paths)
        state = GitHub.ServerMock.get_state()

        case @variant do
          :wrong_attempt ->
            event =
              Repo.one!(
                from(e in CandidateEvent,
                  where: e.candidate_id == ^c.candidate.id and e.kind == "failure_classified"
                )
              )

            evidence = event.details["evidence"] |> Map.put("run_attempt", 2)
            FailureIsolation.classified(batch, "candidate_failure", evidence)

          :wrong_head ->
            p = Enum.at(c.links, 2).patch
            p |> Ecto.Changeset.change(commit: "new-head") |> Repo.update!()

          :moved_main ->
            GitHub.ServerMock.put_state(put_in(state, [conn, :branches, "main"], "new-main"))

          :held ->
            GitHub.ServerMock.put_state(put_in(state, [conn, :labels, 3], ["keep"]))

          :api_outage ->
            GitHub.ServerMock.put_state(put_in(state, [conn, :branches], %{}))

          :manual ->
            p = Enum.at(c.links, 2).patch

            p
            |> Ecto.Changeset.change(manual_approval: %{"head_sha" => "head3"})
            |> Repo.update!()

          :successor ->
            CandidateJournal.event(c.candidate, "successor_reserved", "bors.train", %{
              candidate_id: 999
            })

          :component ->
            hints =
              ownership_hints(c)
              |> Map.put("dependencies_complete", true)
              |> Map.put("import_edges", [[3, 2]])

            CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", %{
              commit: "staged",
              tree: "tree",
              evidence: %{diagnostic_hints: hints}
            })

          :generic ->
            Repo.delete_all(from(e in CandidateEvent, where: e.kind == "failure_diagnostics"))

          _ ->
            :ok
        end

        assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
        assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 0
        refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "provisional_repair"))
        children = Repo.all(from(b in Batch, where: b.id != ^batch.id))

        assert Enum.sort(Enum.flat_map(children, &stored_links(&1.id)) |> Enum.map(& &1.head_sha)) ==
                 Enum.sort(Enum.map(c.links, & &1.head_sha))
      end
    end

    for stop <- [:new_head, :pause, :withdrawal, :api_outage] do
      @stop stop
      test "unconfirmed fallback respects #{@stop}", c do
        alias BorsNG.Worker.SourceCompatibility
        {batch, conn} = provisional_candidate(c)
        assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
        p = Repo.get!(Patch, Enum.at(c.links, 2).patch_id)

        r =
          put_in(
            p.source_compatibility,
            ["provisional", "requested_at"],
            System.system_time(:second) - 5701
          )

        p = p |> Ecto.Changeset.change(source_compatibility: r) |> Repo.update!()
        state = GitHub.ServerMock.get_state()

        case @stop do
          :new_head ->
            p |> Ecto.Changeset.change(commit: "new-head") |> Repo.update!()

          :pause ->
            GitHub.ServerMock.put_state(put_in(state, [conn, :labels, 3], ["human"]))

          :withdrawal ->
            p
            |> Ecto.Changeset.change(
              queue_approval: %{
                "state" => "withdrawn",
                "reason" => "requested",
                "head_sha" => "head3"
              }
            )
            |> Repo.update!()

          :api_outage ->
            GitHub.ServerMock.put_state(put_in(state, [conn, :branches], %{}))
        end

        assert :unavailable =
                 SourceCompatibility.provisional_fallback(
                   c.project,
                   conn,
                   Repo.get!(Patch, p.id),
                   "base"
                 )

        assert Repo.aggregate(Batch, :count, :id) == 2

        refute get_in(Repo.get!(Patch, p.id).source_compatibility, [
                 "provisional",
                 "fallback_batch_id"
               ])
      end
    end

    test "a real isolated failure upgrades provenance without resetting repair or fallback", c do
      alias BorsNG.Worker.SourceCompatibility
      {batch, conn} = provisional_candidate(c)
      assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
      p = Repo.get!(Patch, Enum.at(c.links, 2).patch_id)

      r =
        put_in(
          p.source_compatibility,
          ["provisional", "requested_at"],
          System.system_time(:second) - 5701
        )

      p = p |> Ecto.Changeset.change(source_compatibility: r) |> Repo.update!()
      assert {:ok, _} = SourceCompatibility.provisional_fallback(c.project, conn, p, "base")
      p = Repo.get!(Patch, p.id)
      fallback = Repo.get!(Batch, p.source_compatibility["provisional"]["fallback_batch_id"])
      links = stored_links(fallback.id)
      candidate = CandidateJournal.begin(fallback, links, "base")

      CandidateJournal.bind(
        candidate,
        "isolated",
        "isolated-tree",
        "base",
        %BorsToml{status: ["build"]},
        %{}
      )

      fallback = %{fallback | commit: "isolated", base_sha: "base"}

      evidence = %{
        conclusion: "failure",
        run_id: 101,
        diagnostic: nil,
        failed_steps: [
          %{name: "Build exact candidate under bwrap with workflow-pinned audits, offline"}
        ]
      }

      FailureIsolation.classified(fallback, "candidate_failure", evidence)
      SourceCompatibility.proven_failure(fallback, links, conn)
      p = Repo.get!(Patch, p.id)
      assert p.source_compatibility["status"] == "integrated_failure"
      assert p.source_compatibility["repair_policy"] == "authoritative-individual-v1"
      assert p.source_compatibility["provisional"]["fallback_batch_id"] == fallback.id
      assert SourceCompatibility.requested?(p, "head3")
      assert :unavailable = SourceCompatibility.provisional_fallback(c.project, conn, p, "base")
      assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 2
      SourceCompatibility.proven_failure(fallback, links, conn)
      assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 2
    end

    test "unconfirmed no-op gets one sealed fallback, never a cheap pass or new repair attempt",
         c do
      alias BorsNG.Worker.SourceCompatibility
      {batch, conn} = provisional_candidate(c)
      assert :retrying = FailureIsolation.split(c.links, batch, nil, conn)
      p = Repo.get!(Patch, Enum.at(c.links, 2).patch_id)
      assert :unavailable = SourceCompatibility.provisional_fallback(c.project, conn, p, "base")

      r =
        put_in(
          p.source_compatibility,
          ["provisional", "requested_at"],
          System.system_time(:second) - 5701
        )

      p = p |> Ecto.Changeset.change(source_compatibility: r) |> Repo.update!()
      assert {:ok, _} = SourceCompatibility.provisional_fallback(c.project, conn, p, "base")
      p = Repo.get!(Patch, p.id)
      fallback = Repo.get!(Batch, p.source_compatibility["provisional"]["fallback_batch_id"])
      assert fallback.sealed
      assert prs([stored_links(fallback.id)]) == [[3]]
      assert SourceCompatibility.requested?(p, "head3")
      assert p.source_compatibility["status"] == "suspected_failure"
      assert :unavailable = SourceCompatibility.provisional_fallback(c.project, conn, p, "base")
      # A cheap header PASS never resolves compiler suspicion or authenticates a repair.
      result = %{
        "pr" => 3,
        "head_sha" => "head3",
        "base_sha" => "base",
        "policy" => "local-source-v3",
        "status" => "pass"
      }

      SourceCompatibility.apply_results(c.project, conn, "base", [p], [result])
      p = Repo.get!(Patch, p.id)
      assert SourceCompatibility.requested?(p, "head3")
      assert p.source_compatibility["repair_policy"] == "provisional-diagnostic-v1"

      assert SourceCompatibility.admit(
               c.project,
               conn,
               fallback,
               stored_links(fallback.id),
               "base"
             )
             |> length() == 1

      assert Repo.aggregate(BorsNG.Database.OrchestrationJob, :count, :id) == 1
      [event] = Repo.all(from(e in CandidateEvent, where: e.kind == "provisional_unconfirmed"))
      refute event.details["proof_authority"]
      assert event.details["fallback_batch_id"] == fallback.id
    end
  end

  defp provisional_candidate(c, paths \\ ["TauCeti/File3.lean"]) do
    {batch, evidence} = terminal(c)

    batch =
      Repo.get!(Batch, batch.id)
      |> Batch.changeset(%{state: :running, commit: batch.commit, base_sha: batch.base_sha})
      |> Repo.update!()

    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", %{
      commit: "staged",
      tree: "tree",
      evidence: %{diagnostic_hints: ownership_hints(c)}
    })

    assert :ok =
             BorsNG.Worker.FailureDiagnostics.observe(batch, evidence, :conn, fn _, [200] ->
               log_result(paths)
             end)

    FailureIsolation.classified(batch, "candidate_failure", evidence)
    CandidateJournal.event(c.candidate, "train_reserved", "bors.train", %{cap: 3})
    conn = {{:installation, 91}, 14}

    GitHub.ServerMock.put_state(%{
      conn => %{
        local_git_mode: "local",
        branches: %{"main" => "base"},
        pulls:
          Map.new(
            1..4,
            &{&1, %GitHub.Pr{number: &1, state: :open, base_ref: "main", head_sha: "head#{&1}"}}
          ),
        labels: Map.new(1..4, &{&1, []}),
        comments: Map.new(1..4, &{&1, []})
      }
    })

    {%{batch | project: c.project}, conn}
  end

  defp provisional_mock(_c) do
    conn = {{:installation, 91}, 14}

    GitHub.ServerMock.put_state(%{
      conn => %{
        local_git_mode: "local",
        branches: %{"main" => "base"},
        pulls:
          Map.new(
            1..4,
            &{&1, %GitHub.Pr{number: &1, state: :open, base_ref: "main", head_sha: "head#{&1}"}}
          ),
        labels: Map.new(1..4, &{&1, []}),
        comments: Map.new(1..4, &{&1, []})
      }
    })

    conn
  end

  defp collision_sources(binding, paths) do
    %{
      "complete" => true,
      "policy" => "collision-source-identity-v1",
      "binding" => binding,
      "authority" => "retry_only",
      "proof_authority" => false,
      "files" =>
        Enum.map(paths, fn path ->
          entry = %{
            "blob" => String.duplicate(if(path == "TauCeti/Base.lean", do: "a", else: "b"), 40),
            "bytes" => 100
          }

          %{
            "path" => path,
            "candidate" => entry,
            "base" => if(path == "TauCeti/Base.lean", do: entry)
          }
        end)
    }
  end

  defp base_collision_terminal(c, source_change \\ &Function.identity/1) do
    {batch, evidence, hints} = provider_terminal(c)

    batch =
      Repo.get!(Batch, batch.id)
      |> Batch.changeset(%{state: :running, commit: "staged", base_sha: "base"})
      |> Repo.update!()

    batch = %{batch | project: c.project}
    hints = hints |> Map.put("dependencies_complete", true) |> Map.put("import_edges", [[2, 3]])

    CandidateJournal.event(c.candidate, "local_assembled", "bors.local_git", %{
      commit: "staged",
      tree: "tree",
      evidence: %{diagnostic_hints: hints}
    })

    logs =
      log_result(["TauCeti/File3.lean", "TauCeti/Base.lean"], "duplicate_declaration")
      |> put_in(["jobs", Access.at(0), "bytes"], 1000)

    assert :ok =
             BorsNG.Worker.FailureDiagnostics.observe(
               batch,
               evidence,
               :conn,
               fn _, [200] -> logs end,
               fn _, _, _, _ ->
                 flunk("provider lookup must not supply direct collision evidence")
               end,
               fn project, :conn, binding, paths ->
                 assert project.id == c.project.id
                 collision_sources(binding, paths) |> source_change.()
               end
             )

    FailureIsolation.classified(batch, "candidate_failure", evidence)
    {batch, evidence, hints, logs}
  end

  test "verified base collision preserves direct owner before advisory component widening and replays children",
       c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence, hints, _} = base_collision_terminal(c)
    assert {:ok, 3, proof} = FailureDiagnostics.base_collision(c.candidate, hints)
    assert proof["sources"]["policy"] == "collision-source-identity-v1"
    before = Map.new(c.links, &{&1.patch_id, CandidateJournal.admission(&1.id).snapshot})

    assert {:error, :crash} =
             Repo.transaction(fn ->
               assert :retrying = FailureIsolation.split(c.links, batch)
               Repo.rollback(:crash)
             end)

    assert Repo.aggregate(Batch, :count, :id) == 1
    refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "failure_partition"))

    assert {:ok, :retrying} =
             Repo.transaction(fn ->
               assert :retrying = FailureIsolation.split(Enum.reverse(c.links), batch)
               batch |> Batch.changeset(%{state: :error}) |> Repo.update!()
               :retrying
             end)

    children = Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
    groups = Enum.map(children, &stored_links(&1.id))
    assert prs(groups) == [[3], [1, 2, 4]]
    assert Enum.all?(children, & &1.sealed)

    for l <- List.flatten(groups) do
      a = CandidateJournal.admission(l.id).snapshot

      assert Map.take(
               a,
               ~w(pr patch_id head_sha reviewer manual_approval bundle_id original_admitted_at scheduling)
             ) ==
               Map.take(
                 before[l.patch_id],
                 ~w(pr patch_id head_sha reviewer manual_approval bundle_id original_admitted_at scheduling)
               )

      assert a["parent_batch_id"] == batch.id
      assert a["parent_candidate_id"] == c.candidate.id
    end

    [partition] = Repo.all(from(e in CandidateEvent, where: e.kind == "failure_partition"))
    assert partition.details["groups"] == [[3], [1, 2, 4]]
    assert partition.details["verified_base_collision"]["pr"] == 3
    assert partition.details["authority"] == "retry_only"

    assert Enum.all?(
             Repo.all(Patch),
             &(&1.queue_pending and &1.queue_approval["state"] == "approved")
           )

    refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "provisional_repair"))
    Repo.get!(Batch, batch.id) |> Batch.changeset(%{state: :running}) |> Repo.update!()

    assert :ok =
             FailureDiagnostics.observe(
               batch,
               evidence,
               :conn,
               fn _, _ -> flunk("re-read logs") end,
               fn _, _, _, _ -> flunk("re-read provider") end,
               fn _, _, _, _ -> flunk("re-read sources") end
             )

    assert :retrying = FailureIsolation.split(c.links, batch)
    assert Repo.aggregate(Batch, :count, :id) == 3
  end

  test "source identity failure retains previous conservative component partition", c do
    alias BorsNG.Worker.FailureDiagnostics

    {batch, _, hints, _} =
      base_collision_terminal(c, fn _ -> %{"complete" => false, "reason" => "mirror_busy"} end)

    assert :unavailable = FailureDiagnostics.base_collision(c.candidate, hints)
    assert :retrying = FailureIsolation.split(c.links, batch)

    assert prs(
             Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
             |> Enum.map(&stored_links(&1.id))
           ) == [[2, 3], [1, 4]]
  end

  test "base collision pure mapping excludes other owners, mixed findings and imperfect identities",
       c do
    alias BorsNG.Worker.FailureDiagnostics
    {_, evidence, hints, logs} = base_collision_terminal(c)
    binding = FailureDiagnostics.binding(c.candidate, "staged", evidence)
    paths = ["TauCeti/Base.lean", "TauCeti/File3.lean"]
    sources = collision_sources(binding, paths)
    assert {:ok, 3} = FailureDiagnostics.map_base_collision(hints, logs, binding, sources)

    assert {:ok, groups, "compiler_diagnostics"} =
             FailureIsolation.partition(
               c.links,
               hints
               |> Map.put("compiler_suspects", [3])
               |> Map.put("verified_base_collision", %{"pr" => 3}),
               nil
             )

    assert prs(groups) == [[3], [1, 2, 4]]

    same_owner =
      put_in(hints, ["changed_files", Access.at(2), "paths"], [
        "TauCeti/Base.lean",
        "TauCeti/File3.lean"
      ])

    assert :unavailable =
             FailureDiagnostics.map_base_collision(same_owner, logs, binding, sources)

    overlap =
      put_in(hints, ["changed_files", Access.at(1), "paths"], [
        "TauCeti/File2.lean",
        "TauCeti/Shared.lean"
      ])
      |> update_in(["changed_files", Access.at(2), "paths"], &(&1 ++ ["TauCeti/Shared.lean"]))
      |> Map.put("compiler_suspects", [3])
      |> Map.put("verified_base_collision", %{"pr" => 3})

    assert {:ok, shared_groups, "compiler_diagnostics"} =
             FailureIsolation.partition(c.links, overlap, nil)

    assert prs(shared_groups) == [[2, 3], [1, 4]]
    two_owners = put_in(hints, ["changed_files", Access.at(0), "paths"], ["TauCeti/Base.lean"])
    shared = put_in(hints, ["changed_files", Access.at(0), "paths"], ["TauCeti/File3.lean"])

    dependency =
      put_in(hints, ["changed_files", Access.at(0), "dependency_paths"], ["TauCeti/Base.lean"])

    mixed =
      update_in(
        logs,
        ["jobs", Access.at(0), "observations"],
        &(&1 ++ [%{"kind" => "compiler_error", "paths" => ["TauCeti/File2.lean"]}])
      )

    pairs =
      update_in(
        logs,
        ["jobs", Access.at(0), "observations"],
        &(&1 ++
            [
              %{
                "kind" => "duplicate_declaration",
                "paths" => ["TauCeti/File2.lean", "TauCeti/Other.lean"]
              }
            ])
      )

    for {h, l, b, src} <- [
          {two_owners, logs, binding, sources},
          {shared, logs, binding, sources},
          {dependency, logs, binding, sources},
          {hints, mixed, binding, sources},
          {hints, pairs, binding, sources},
          {hints, Map.put(logs, "complete", false), binding, sources},
          {hints, put_in(logs, ["jobs", Access.at(0), "bytes"], nil), binding, sources},
          {hints, logs, binding, Map.put(sources, "complete", false)},
          {hints, logs, binding, put_in(sources, ["files", Access.at(0), "base"], nil)},
          {hints, logs, binding,
           put_in(sources, ["files", Access.at(0), "base", "blob"], String.duplicate("c", 40))},
          {hints, logs, binding,
           put_in(
             sources,
             ["files", Access.at(1), "base"],
             sources["files"] |> Enum.at(1) |> Map.get("candidate")
           )},
          {hints, logs, binding,
           put_in(sources, ["files", Access.at(0), "candidate", "bytes"], 262_145)},
          {hints, logs, binding, Map.put(sources, "policy", "bounded-provider-hint-v1")},
          {hints, logs, Map.put(binding, "run_attempt", 2), sources},
          {hints, logs, Map.put(binding, "generation", 2), sources},
          {hints, logs, Map.put(binding, "base_sha", "other"), sources},
          {Map.delete(hints, "provenance"), logs, binding, sources},
          {Map.put(hints, "dependencies_complete", false), logs, binding, sources},
          {Map.put(hints, "missing_imports", [%{"pr" => 2}]), logs, binding, sources},
          {put_in(hints, ["changed_files", Access.at(0), "head_sha"], "other"), logs, binding,
           sources}
        ] do
      assert :unavailable = FailureDiagnostics.map_base_collision(h, l, b, src)
    end
  end

  test "collision source bypass keeps approved bundles whole and suppresses stronger handoff",
       c do
    {batch, _, hints, _} = base_collision_terminal(c)
    bundle = Repo.insert!(%PatchBundle{project_id: c.project.id})

    Enum.each(Enum.filter(c.links, &(&1.patch.pr_xref in [2, 3])), fn l ->
      l.patch |> Patch.changeset(%{bundle_id: bundle.id}) |> Repo.update!()
    end)

    links = stored_links(batch.id)
    assert :retrying = FailureIsolation.split(links, batch, nil, :no_github_calls)

    groups =
      Repo.all(from(b in Batch, where: b.id != ^batch.id, order_by: b.id))
      |> Enum.map(&stored_links(&1.id))

    assert prs(groups) == [[2, 3], [1, 4]]
    refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "provisional_repair"))
    assert {:ok, 3, _} = BorsNG.Worker.FailureDiagnostics.base_collision(c.candidate, hints)
  end

  test "base collision cannot reuse changed classifier or live member state", c do
    alias BorsNG.Worker.FailureDiagnostics
    {batch, evidence, hints, _} = base_collision_terminal(c)

    for changed <- [
          %{evidence | run_id: 101},
          %{evidence | run_attempt: 2},
          %{evidence | run_created_at: "2000-01-01T00:00:00Z"},
          %{evidence | identity: "wrong"},
          %{evidence | conclusion: "success"},
          put_in(evidence, [:failed_steps, Access.at(0), :job_id], 201)
        ] do
      FailureIsolation.classified(batch, "candidate_failure", changed)
      assert :unavailable = FailureDiagnostics.base_collision(c.candidate, hints)
    end

    FailureIsolation.classified(batch, "candidate_failure", evidence)
    hd(c.links).patch |> Patch.changeset(%{commit: "new"}) |> Repo.update!()
    assert :unavailable = FailureDiagnostics.base_collision(c.candidate, hints)
  end

  test "frozen real candidate233 changes only scheduling2/33 to1/34 with exact snapshots" do
    alias BorsNG.Worker.FailureDiagnostics
    data = File.read!("test/fixtures/candidate233-base-collision.json") |> Jason.decode!()
    members = data["members"]
    assert length(members) == 35
    assert CandidateJournal.digest(members) == data["binding"]["members_digest"]

    links =
      Enum.map(members, fn m ->
        %LinkPatchBatch{
          id: m["link_id"],
          patch_id: m["patch_id"],
          head_sha: m["head_sha"],
          reviewer: m["reviewer"],
          manual_approval: m["manual_approval"],
          patch: %Patch{
            pr_xref: m["pr"],
            commit: m["head_sha"],
            bundle_id: m["bundle_id"],
            is_single: get_in(m, ["scheduling", "singleton"]) == true
          }
        }
      end)

    assert {:ok, [16504]} = FailureDiagnostics.map_paths(data["hints"], data["logs"])
    hints = Map.put(data["hints"], "compiler_suspects", [16504])
    assert {:ok, baseline, "compiler_diagnostics"} = FailureIsolation.partition(links, hints, nil)
    assert Enum.map(baseline, &length/1) == [2, 33]
    assert hd(prs(baseline)) == [16504, 16650]

    assert {:ok, 16504} =
             FailureDiagnostics.map_base_collision(
               data["hints"],
               data["logs"],
               data["binding"],
               data["collision_sources"]
             )

    hints = Map.put(hints, "verified_base_collision", %{"pr" => 16504})
    assert {:ok, groups, "compiler_diagnostics"} = FailureIsolation.partition(links, hints, nil)
    assert Enum.map(groups, &length/1) == [1, 34]
    assert hd(prs(groups)) == [16504]
    assert 16650 in List.last(prs(groups))

    snapshots =
      Enum.flat_map(groups, fn group ->
        Enum.map(group, fn link -> Enum.find(members, &(&1["link_id"] == link.id)) end)
      end)

    assert Enum.sort_by(snapshots, & &1["link_id"]) == Enum.sort_by(members, & &1["link_id"])
  end
end
