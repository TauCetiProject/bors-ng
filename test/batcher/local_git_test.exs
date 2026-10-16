defmodule BorsNG.Worker.LocalGitTest do
  use ExUnit.Case, async: false
  alias BorsNG.Worker.LocalGit
  alias BorsNG.Database.{Candidate, Project}
  alias BorsNG.GitHub.ServerMock

  test "real Git assembly, conflicts, immutable inputs, restart, concurrency and leased push" do
    {output, status} = System.cmd("python3", ["test/local_git_test.py"], stderr_to_stdout: true)
    assert status == 0, output
    assert output =~ ~r/Ran [1-9][0-9]* tests/
  end

  test "bounded terminal log parser and credential-isolated retrieval" do
    {output, status} =
      System.cmd("python3", ["test/failure_diagnostics_test.py"], stderr_to_stdout: true)

    assert status == 0, output
  end

  test "collision identity never requests GitHub credentials or bootstraps a cold mirror" do
    root = Path.join(System.tmp_dir!(), "bors-collision-#{System.unique_integer([:positive])}")
    old = System.get_env("BORS_GIT_MIRROR_ROOT")
    System.put_env("BORS_GIT_MIRROR_ROOT", root)

    on_exit(fn ->
      if old,
        do: System.put_env("BORS_GIT_MIRROR_ROOT", old),
        else: System.delete_env("BORS_GIT_MIRROR_ROOT")

      File.rm_rf!(root)
    end)

    binding = %{
      candidate_id: 1,
      generation: 1,
      members_digest: String.duplicate("a", 64),
      commit: String.duplicate("b", 40),
      base_sha: String.duplicate("c", 40),
      run_id: 100,
      run_attempt: 1,
      job_ids: [200],
      workflow: ".github/workflows/pr-build.yml"
    }

    assert %{"status" => "error", "details" => %{"kind" => "mirror_unavailable"}} =
             LocalGit.collision_sources(%Project{name: "test/repo"}, :no_credentials, binding, [
               "TauCeti/Base.lean",
               "TauCeti/New.lean"
             ])

    refute File.exists?(root)
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

  test "assembly capacity supports the configured maximum and rejects excess before helper work" do
    for count <- [32, 33, 128], do: assert(LocalGit.capacity(Enum.to_list(1..count)) == :ok)

    assert {:error, %{"kind" => "member_limit", "max_members" => 128}} =
             LocalGit.capacity(Enum.to_list(1..129))
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
