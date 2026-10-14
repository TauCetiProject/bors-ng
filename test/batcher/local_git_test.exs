defmodule BorsNG.Worker.LocalGitTest do
  use ExUnit.Case, async: false
  alias BorsNG.Worker.LocalGit
  alias BorsNG.Database.{Candidate, Project}
  alias BorsNG.GitHub.ServerMock

  test "real Git assembly, conflicts, immutable inputs, restart, concurrency and leased push" do
    {output, status} = System.cmd("python3", ["test/local_git_test.py"], stderr_to_stdout: true)
    assert status == 0, output
    assert output =~ "Ran 11 tests"
  end

  test "only the scoped main project can select local assembly" do
    conn = {{:installation, 91}, 14}
    ServerMock.put_state(%{conn => %{local_git_mode: "local"}})
    project = %Project{name: "TauCetiProject/TauCeti"}
    assert LocalGit.mode(project, "main", conn) == "local"
    assert LocalGit.mode(project, "pilot", conn) == "off"
    assert LocalGit.mode(%{project | name: "other/project"}, "main", conn) == "off"
    ServerMock.put_state(%{conn => %{local_git_mode: "shadow"}})
    assert LocalGit.mode(project, "main", conn) == "shadow"
    ServerMock.put_state(%{conn => %{local_git_mode: "invalid"}})
    assert LocalGit.mode(project, "main", conn) == "off"
  end

  test "metadata cannot reorder or replace approved immutable members" do
    candidate = %Candidate{
      id: 1,
      generation: 1,
      snapshot: %{
        "base_sha" => "base",
        "members" => [
          %{"pr" => 1, "assembly_head_sha" => "one", "head_sha" => "one"},
          %{"pr" => 2, "assembly_head_sha" => "two", "head_sha" => "two"}
        ]
      }
    }

    for members <- [
          [%{pr: 1, source_sha: "new"}, %{pr: 2, source_sha: "two"}],
          [%{pr: 2, source_sha: "two"}, %{pr: 1, source_sha: "one"}]
        ] do
      assert {:error, %{kind: "input_identity_mismatch"}} =
               LocalGit.assemble(candidate, %{members: members})
    end

    candidate =
      put_in(candidate.snapshot["members"], [
        %{"pr" => 1, "assembly_head_sha" => "one", "head_sha" => nil}
      ])

    assert {:error, %{kind: "input_identity_mismatch"}} =
             LocalGit.assemble(candidate, %{members: [%{pr: 1, source_sha: "one"}]})
  end
end
