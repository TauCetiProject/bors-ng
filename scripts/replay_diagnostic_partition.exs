# MIX_ENV=test BORS_DATABASE=postgresql mix run --no-start scripts/replay_diagnostic_partition.exs INPUT OUTPUT
# Uses the production pure planner; never starts the application or writes queue state.
alias BorsNG.Database.{LinkPatchBatch, Patch}
alias BorsNG.Worker.{FailureDiagnostics, FailureIsolation}

[input, output] = System.argv()
data = input |> File.read!() |> Jason.decode!()
members = data["members"]

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

hints =
  case FailureDiagnostics.map_paths(data["hints"], data["logs"]) do
    {:ok, suspects} ->
      Map.put(data["hints"], "compiler_suspects", suspects)

    _ ->
      case FailureDiagnostics.map_retry_providers(
             data["hints"],
             data["logs"],
             data["binding"],
             data["provider_lookup"]
           ) do
        {:ok, suspects, "import_provider_hint"} ->
          Map.put(data["hints"], "provider_suspects", suspects)

        {:ok, suspects, "bounded_provider_hint"} ->
          Map.put(data["hints"], "bounded_provider_suspects", suspects)

        _ ->
          data["hints"]
      end
  end

# Optional source identities are a separate local assessment, never a retroactive
# production observation. Verify the frozen member digest before consuming them.
hints =
  if data["collision_sources"] do
    true = data["binding"]["candidate_id"] == data["candidate_id"]
    true = data["binding"]["members_digest"] == BorsNG.Worker.CandidateJournal.digest(members)
    true = Enum.all?(members, &(&1["head_sha"] == &1["assembly_head_sha"]))

    case FailureDiagnostics.map_base_collision(
           data["hints"],
           data["logs"],
           data["binding"],
           data["collision_sources"]
         ) do
      {:ok, pr} -> Map.put(hints, "verified_base_collision", %{"pr" => pr})
      _ -> hints
    end
  else
    hints
  end

result =
  case FailureIsolation.partition(links, hints, nil) do
    {:ok, groups, reason} ->
      snapshots =
        Enum.map(groups, fn group ->
          Enum.map(group, fn link -> Enum.find(members, &(&1["link_id"] == link.id)) end)
        end)

      true =
        Enum.sort_by(List.flatten(snapshots), & &1["link_id"]) ==
          Enum.sort_by(members, & &1["link_id"])

      %{
        strategy: reason,
        groups: Enum.map(snapshots, &Enum.map(&1, fn m -> m["pr"] end)),
        snapshots: snapshots
      }

    :fallback ->
      %{
        strategy: "deterministic_bisection",
        reason: "unknown, ambiguous or non-narrowing diagnostics"
      }
  end

result =
  Map.merge(result, %{
    candidate_id: data["candidate_id"],
    members_preserved: length(members),
    authority: "diagnostic_replay_only",
    proof_authority: false
  })

File.write!(output, Jason.encode!(result, pretty: true) <> "\n")
IO.puts(Jason.encode!(Map.drop(result, [:snapshots])))
