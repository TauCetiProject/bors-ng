defmodule BorsNG.GitHub.Advisory do
  @moduledoc "Single advisory API client with an isolated token cache and hard local deadline."
  use GenServer
  alias BorsNG.GitHub

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  def init(state), do: {:ok, state}

  def handle_call({:advisory, _, _, _, deadline} = request, from, state) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:reply, {:error, :advisory_request}, state}
    else
      task =
        Task.async(fn ->
          if Confex.fetch_env!(:bors, :server) == GitHub.Server do
            GitHub.Server.handle_call(request, from, state)
          else
            # Preserve the shared deterministic mock state in tests.
            try do
              {:reply, GenServer.call(GitHub, request, remaining), state}
            catch
              _, _ -> {:reply, {:error, :advisory_request}, state}
            end
          end
        end)

      case Task.yield(task, remaining) || Task.shutdown(task, :brutal_kill) do
        {:ok, {:reply, response, cache}} -> {:reply, response, cache}
        _ -> {:reply, {:error, :advisory_request}, state}
      end
    end
  end
end
