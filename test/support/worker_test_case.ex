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

  # Keep SQL ownership alive while globally supervised fixture actors stop.
  # Otherwise a final asynchronous webhook cast can outlive its test process
  # and take the application down through repeated sandbox ownership exits.
  def stop_actors do
    for supervisor <- [BorsNG.Worker.Batcher.Supervisor, BorsNG.Worker.Attemptor.Supervisor] do
      if Process.whereis(supervisor), do: supervisor.terminate_all()
    end
  end

  setup tags do
    repo = :persistent_term.get(:db_repo)
    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(repo, shared: not tags[:async])

    on_exit(fn ->
      unless tags[:async], do: stop_actors()
      Ecto.Adapters.SQL.Sandbox.stop_owner(owner)
    end)

    :ok
  end
end
