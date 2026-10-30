defmodule BorsNG.Worker.Orchestration.Supervisor do
  use Supervisor
  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  def init(_) do
    Supervisor.init(
      [
        {BorsNG.GitHub.Advisory, []},
        Supervisor.child_spec(
          {BorsNG.GitHub.Advisory, [name: BorsNG.GitHub.Cleanup]},
          id: BorsNG.GitHub.Cleanup
        ),
        {Task.Supervisor, name: BorsNG.GitHub.ReadTasks},
        {BorsNG.GitHub.ReadOnly, []},
        {Registry, keys: :unique, name: BorsNG.Worker.Orchestration.Registry},
        {DynamicSupervisor, strategy: :one_for_one, name: BorsNG.Worker.Orchestration.Workers},
        {BorsNG.Worker.Orchestration.Dispatcher, []}
      ],
      strategy: :one_for_all
    )
  end
end
