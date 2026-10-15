defmodule BorsNG.Worker.TestCase do
  @moduledoc """
  This module defines the test case to be used by
  tests that require setting up a connection.

  Finally, if the test case interacts with the database,
  it cannot be async. For this reason, every test runs
  inside a transaction which is reset at the beginning
  of the test unless the test case is marked as async.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      alias BorsNG.Database
      import Ecto
      import Ecto.Changeset
      import Ecto.Query
    end
  end

  setup tags do
    owner =
      Ecto.Adapters.SQL.Sandbox.start_owner!(
        :persistent_term.get(:db_repo),
        shared: not tags[:async]
      )

    on_exit(fn ->
      # Keep the transaction owner alive while fixture workers shut down.
      # Otherwise a late eligibility/cancellation can lose its connection and
      # crash a shared registry during the following test.
      unless tags[:async] do
        for supervisor <- [
              BorsNG.Worker.Batcher.Supervisor,
              BorsNG.Worker.Attemptor.Supervisor,
              BorsNG.Worker.Orchestration.Workers
            ] do
          for {_, pid, _, _} <- DynamicSupervisor.which_children(supervisor), is_pid(pid) do
            DynamicSupervisor.terminate_child(supervisor, pid)
          end
        end

        for registry <- [BorsNG.Worker.Batcher.Registry, BorsNG.Worker.Attemptor.Registry] do
          try do
            :sys.get_state(registry, 1_000)
          catch
            :exit, _ -> :ok
          end
        end
      end

      Ecto.Adapters.SQL.Sandbox.stop_owner(owner)
    end)

    :ok
  end
end
