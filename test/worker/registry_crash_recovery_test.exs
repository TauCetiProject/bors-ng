defmodule BorsNG.Worker.RegistryCrashRecoveryTest do
  @moduledoc """
  Regression tests for the worker registries' crash handling.

  When a per-project worker crashes, its registry handles the `:DOWN` and
  records a `Crash` row. If recording fails — e.g. the project row is gone, so
  the insert violates `crashes_project_id_fkey` — the handler must NOT raise:
  doing so previously took the whole registry down, dropping its map of every
  worker pid it tracks. These tests drive the `:DOWN` handler directly with a
  state pointing at a deleted project so the `Crash` insert is guaranteed to
  fail, and assert the handler returns normally instead of crashing.
  """
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.Installation
  alias BorsNG.Database.Project
  alias BorsNG.Database.Repo

  # Insert a project, then delete it, so its id is guaranteed absent and any
  # Crash row referencing it violates the foreign key.
  defp missing_project_id do
    inst = Repo.insert!(%Installation{installation_xref: 7_000_001})

    proj =
      Repo.insert!(%Project{
        installation_id: inst.id,
        repo_xref: 7_000_002,
        staging_branch: "staging"
      })

    id = proj.id
    Repo.delete!(proj)
    id
  end

  test "Attemptor.Registry survives a :DOWN whose Crash insert violates the FK" do
    project_id = missing_project_id()
    ref = make_ref()
    state = {%{}, %{ref => project_id}}

    assert {:noreply, {names, _refs}} =
             BorsNG.Worker.Attemptor.Registry.handle_info(
               {:DOWN, ref, :process, self(), :test_induced_crash},
               state
             )

    # The handler restarts the attemptor before recording the crash; terminate
    # it so it doesn't poll the now-missing project after the test ends.
    case names[project_id] do
      pid when is_pid(pid) ->
        DynamicSupervisor.terminate_child(BorsNG.Worker.Attemptor.Supervisor, pid)

      _ ->
        :ok
    end
  end

  test "Batcher.Registry survives a :DOWN whose Crash insert violates the FK" do
    project_id = missing_project_id()
    ref = make_ref()
    state = {%{}, %{ref => project_id}}

    assert {:noreply, {_names, _refs}} =
             BorsNG.Worker.Batcher.Registry.handle_info(
               {:DOWN, ref, :process, self(), :test_induced_crash},
               state
             )
  end

  test "batcher restart never shortens an existing CI recovery deadline" do
    inst = Repo.insert!(%Installation{installation_xref: 7_000_003})

    project =
      Repo.insert!(%Project{
        name: "recovery",
        installation_id: inst.id,
        repo_xref: 7_000_004,
        staging_branch: "staging"
      })

    deadline = DateTime.to_unix(DateTime.utc_now()) + 3_600

    batch =
      Repo.insert!(%BorsNG.Database.Batch{
        project_id: project.id,
        into_branch: "master",
        state: :waiting,
        last_polled: 0,
        recovery_after: deadline,
        recovery_failures: 1
      })

    ref = make_ref()

    assert {:noreply, {names, _refs}} =
             BorsNG.Worker.Batcher.Registry.handle_info(
               {:DOWN, ref, :process, self(), :test_induced_crash},
               {%{}, %{ref => project.id}}
             )

    observed = Repo.get!(BorsNG.Database.Batch, batch.id)
    assert observed.recovery_after >= deadline
    assert observed.recovery_failures == 2

    if is_pid(names[project.id]),
      do: DynamicSupervisor.terminate_child(BorsNG.Worker.Batcher.Supervisor, names[project.id])
  end
end
