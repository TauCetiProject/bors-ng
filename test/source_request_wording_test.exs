defmodule BorsNG.SourceRequestWordingTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.SourceCompatibility
  @head String.duplicate("a", 40)
  @base String.duplicate("b", 40)

  defp missing(finding) do
    SourceCompatibility.request(@head, %{
      "status" => "missing_source",
      "base_sha" => @base,
      "policy" => "local-source-v3",
      "missing_imports" => [
        Map.merge(
          %{
            "pr" => 1,
            "path" => "TauCeti/Importer.lean",
            "module" => "TauCeti.Provider",
            "provider_path" => "TauCeti/Provider.lean"
          },
          finding
        )
      ]
    })
  end

  test "a removed source is described as the PR's removal, not a missing import" do
    body = missing(%{"removed" => true})
    assert body =~ "removes or renames a module that other sources still import"

    assert body =~
             "- `TauCeti/Importer.lean` imports `TauCeti.Provider` (`TauCeti/Provider.lean`), which this PR removes."

    assert body =~ "keep the module or update every importer"
    refute body =~ "missing from `main`"
  end

  test "an import of a missing module keeps the existing wording" do
    body = missing(%{})
    assert body =~ "imports modules that are missing from `main`"

    assert body =~
             "- `TauCeti/Importer.lean` imports missing `TauCeti.Provider` (`TauCeti/Provider.lean`)."
  end
end
