defmodule BorsNG.Worker.PrivateOriginsTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.{FailureDiagnostics, PrivateOrigins}

  for cid <- [1864, 1914] do
    @cid cid
    test "historical candidate #{@cid} maps the repeated private lint to its exact source owner" do
      f = File.read!("test/fixtures/private-origin-candidate#{@cid}.json") |> Jason.decode!()
      assert :unavailable = FailureDiagnostics.map_paths(f["hints"], f["logs"])
      resolved = PrivateOrigins.resolve(f["hints"], f["logs"], f["binding"], f["source_lookup"])
      assert {:ok, [26212]} = FailureDiagnostics.map_paths(f["hints"], resolved)
      [finding] = hd(resolved["jobs"])["observations"]
      assert finding["origin"] == "verified_private_module"
      assert finding["origin_unknown"] == "unsupported_origin"
      refute Map.has_key?(finding, "line")
      refute Map.has_key?(finding, "column")
      refute f["source_lookup"]["proof_authority"]

      unknown = %{
        "kind" => "environment_linter",
        "paths" => [],
        "unknown" => "missing_origin",
        "declaration" => "TauCeti.unlocated"
      }

      # An unlocated finding beside the located one no longer voids the
      # mapping: the located owner is handed off and the remainder's next
      # build reports what the unlocated finding was about.
      partial = update_in(f["logs"], ["jobs", Access.at(0), "observations"], &[unknown | &1])
      resolved = PrivateOrigins.resolve(f["hints"], partial, f["binding"], f["source_lookup"])
      assert {:ok, [26212]} = FailureDiagnostics.map_paths(f["hints"], resolved)
    end
  end

  test "missing source, mismatched binding, stale heads and ambiguous ownership remain unknown" do
    f = File.read!("test/fixtures/private-origin-candidate1914.json") |> Jason.decode!()
    sources = f["source_lookup"]

    for bad <- [
          nil,
          Map.put(sources, "complete", false),
          Map.put(sources, "proof_authority", true),
          put_in(sources, ["binding", "commit"], String.duplicate("0", 40)),
          put_in(sources, ["binding", "run_attempt"], 99),
          Map.put(sources, "files", []),
          put_in(sources, ["files", Access.at(0), "candidate"], nil),
          put_in(sources, ["files", Access.at(0), "candidate", "bytes"], 256 * 1024 + 1)
        ] do
      assert PrivateOrigins.resolve(f["hints"], f["logs"], f["binding"], bad) == f["logs"]
    end

    hints = f["hints"]
    path = hd(sources["files"])["path"]

    for bad <- [
          put_in(hints, ["provenance", "members", Access.at(0), "head_sha"], "moved"),
          update_in(hints, ["changed_files", Access.at(0), "paths"], &[path | &1]),
          Map.put(hints, "ownership_complete", false)
        ] do
      assert PrivateOrigins.resolve(bad, f["logs"], f["binding"], sources) == f["logs"]
    end
  end

  test "private prefix parsing rejects ambiguous, foreign, truncated and hostile module spellings" do
    finding = %{"kind" => "environment_linter", "paths" => [], "unknown" => "unsupported_origin"}

    for declaration <- [
          "TauCeti.X.foo",
          "_private.Mathlib.X.0.foo",
          "_private.TauCeti.X.foo",
          "_private.TauCeti.X.0.",
          "_private.TauCeti.«X.Y».0.foo",
          "_private.TauCeti/Outside.0.foo",
          "_private.TauCeti.X.0.foo\nforged",
          "_private.TauCeti.X.-1.foo"
        ] do
      assert PrivateOrigins.path(Map.put(finding, "declaration", declaration)) == nil
    end

    assert PrivateOrigins.path(Map.put(finding, "declaration", "_private.TauCeti.X.12.λ雪")) ==
             "TauCeti/X.lean"
  end
end
