defmodule BorsNG.Worker.ImporterOriginsTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.{FailureDiagnostics, ImporterOrigins, SourceCompatibility}

  @slope "TauCeti/LowDimTopology/DehnSurgery/Slope.lean"
  @primitive "TauCeti/Algebra/Module/Primitive.lean"

  defp fixture,
    do: File.read!("test/fixtures/importer-origin-candidate1963.json") |> Jason.decode!()

  defp finding(resolved), do: hd(hd(resolved["jobs"])["observations"])

  test "candidate 1963 attributes the unchanged importer's lint finding to the one member that changed its import" do
    f = fixture()
    assert :unavailable = FailureDiagnostics.map_paths(f["hints"], f["logs"])
    assert {:ok, [@slope]} = ImporterOrigins.paths(f["hints"], f["logs"])

    resolved = ImporterOrigins.resolve(f["hints"], f["logs"], f["binding"], f["importer_lookup"])
    assert {:ok, [26093]} = FailureDiagnostics.map_paths(f["hints"], resolved)

    finding = finding(resolved)
    assert finding["paths"] == [@primitive]
    assert finding["origin"] == "importer_hop"
    assert finding["origin_unknown"] == "unsupported_origin"
    refute Map.has_key?(finding, "unknown")

    assert finding["attributed_via"] == %{
             "path" => @slope,
             "imports" => [@primitive],
             "pr" => 26093
           }

    refute f["importer_lookup"]["proof_authority"]
  end

  test "zero or several members changing the imports leave the finding unmapped" do
    f = fixture()
    lookup = f["importer_lookup"]

    nobody = put_in(lookup, ["files", Access.at(0), "providers"], [])
    assert ImporterOrigins.resolve(f["hints"], f["logs"], f["binding"], nobody) == f["logs"]

    elsewhere =
      put_in(lookup, ["files", Access.at(0), "providers"], [
        %{"path" => "TauCeti/Nobody/Touched.lean", "blob" => String.duplicate("b", 40)}
      ])

    assert ImporterOrigins.resolve(f["hints"], f["logs"], f["binding"], elsewhere) == f["logs"]

    # A second member also changing Primitive.lean makes the hop ambiguous.
    hints =
      update_in(f["hints"], ["changed_files"], fn members ->
        [first | rest] = members
        other = Enum.find(rest, &(&1["pr"] != 26093))

        Enum.map(members, fn m ->
          if m == other, do: Map.update!(m, "paths", &[@primitive | &1]), else: m
        end)
        |> then(fn ms -> [first | tl(ms)] end)
      end)

    assert ImporterOrigins.resolve(hints, f["logs"], f["binding"], lookup) == f["logs"]
    assert :unavailable = FailureDiagnostics.map_paths(hints, f["logs"])
  end

  test "incomplete, unbound or oversized lookups leave the finding unmapped" do
    f = fixture()
    lookup = f["importer_lookup"]

    for bad <- [
          nil,
          %{"status" => "error", "details" => %{"kind" => "helper_exit"}},
          Map.put(lookup, "complete", false),
          Map.put(lookup, "proof_authority", true),
          put_in(lookup, ["binding", "commit"], String.duplicate("0", 40)),
          put_in(lookup, ["binding", "run_attempt"], 99),
          Map.put(lookup, "files", []),
          put_in(lookup, ["files", Access.at(0), "path"], "TauCeti/Other.lean"),
          put_in(lookup, ["files", Access.at(0), "bytes"], 256 * 1024 + 1),
          put_in(lookup, ["files", Access.at(0), "providers", Access.at(0), "path"], "../x"),
          update_in(lookup, ["files", Access.at(0), "providers"], fn p ->
            p ++
              for i <- 1..129,
                  do: %{"path" => "TauCeti/P#{i}.lean", "blob" => String.duplicate("c", 40)}
          end)
        ] do
      assert ImporterOrigins.resolve(f["hints"], f["logs"], f["binding"], bad) == f["logs"]
    end

    hints = f["hints"]
    moved = put_in(hints, ["provenance", "members", Access.at(0), "head_sha"], "moved")
    assert ImporterOrigins.resolve(moved, f["logs"], f["binding"], lookup) == f["logs"]
    incomplete = Map.put(hints, "ownership_complete", false)
    assert :unavailable = ImporterOrigins.paths(incomplete, f["logs"])
    assert ImporterOrigins.resolve(incomplete, f["logs"], f["binding"], lookup) == f["logs"]
  end

  test "an owned path, a located finding and a compiler error are not hopped" do
    f = fixture()

    owned = update_in(f["hints"], ["changed_files", Access.at(0), "paths"], &[@slope | &1])
    assert :unavailable = ImporterOrigins.paths(owned, f["logs"])

    compiler =
      put_in(f["logs"], ["jobs", Access.at(0), "observations"], [
        %{"kind" => "compiler_error", "paths" => [@slope]}
      ])

    assert :unavailable = ImporterOrigins.paths(f["hints"], compiler)

    assert ImporterOrigins.resolve(f["hints"], compiler, f["binding"], f["importer_lookup"]) ==
             compiler
  end

  test "a non-private finding that names an unchanged module takes the same hop" do
    f = fixture()

    logs =
      put_in(f["logs"], ["jobs", Access.at(0), "observations"], [
        %{
          "kind" => "environment_linter",
          "linter" => "unusedArguments",
          "declaration" => "TauCeti.slopeSetoid",
          "paths" => [@slope],
          "line" => 7,
          "column" => 25
        }
      ])

    assert {:ok, [@slope]} = ImporterOrigins.paths(f["hints"], logs)
    resolved = ImporterOrigins.resolve(f["hints"], logs, f["binding"], f["importer_lookup"])
    assert {:ok, [26093]} = FailureDiagnostics.map_paths(f["hints"], resolved)
    finding = finding(resolved)
    assert finding["paths"] == [@primitive]
    assert finding["attributed_via"]["path"] == @slope
    refute Map.has_key?(finding, "origin_unknown")
  end

  test "the repair request names the unchanged module and the imported file" do
    f = fixture()
    resolved = ImporterOrigins.resolve(f["hints"], f["logs"], f["binding"], f["importer_lookup"])

    result = %{
      "status" => "suspected_failure",
      "base_sha" => f["binding"]["base_sha"],
      "policy" => "provisional-diagnostic-v1",
      "run_url" => "https://github.com/TauCetiProject/TauCeti/actions/runs/1",
      "provisional" => %{
        "candidate_id" => 1963,
        "batch_id" => 2654,
        "binding" => f["binding"],
        "diagnostics" => resolved
      }
    }

    text = SourceCompatibility.request(String.duplicate("a", 40), result)
    assert text =~ "- `#{@primitive}`"

    assert text =~
             "The failure was reported in `#{@slope}`, which imports this PR's `#{@primitive}`."

    plain = put_in(result, ["provisional", "diagnostics"], f["logs"])
    refute SourceCompatibility.request(String.duplicate("a", 40), plain) =~ "which imports"
  end
end
