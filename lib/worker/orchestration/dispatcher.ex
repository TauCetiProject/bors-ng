defmodule BorsNG.Worker.Orchestration.Dispatcher do
  use GenServer
  require Logger
  alias BorsNG.Worker.Orchestration.{Outbox, Worker}

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_), do: {:ok, %{active: %{}}, {:continue, :start}}

  def handle_continue(:start, state) do
    Process.send_after(self(), :tick, 1000)
    {:noreply, state}
  end

  def handle_info(:tick, state) do
    # At most one timer, regardless of webhook rate.
    if timer = Process.get(:tick_timer), do: Process.cancel_timer(timer)
    Process.put(:tick_timer, Process.send_after(self(), :tick, 1000))
    now = System.monotonic_time(:millisecond)
    last = Process.get(:last_drain_at)

    state =
      if Outbox.enabled?() and (is_nil(last) or now - last >= 100) do
        Process.put(:last_drain_at, now)
        drain(state)
      else
        state
      end

    {:noreply, state}
  end

  def handle_info({:finished, pid, job, result}, state) do
    case Map.get(state.active, pid) do
      {ref, %{token: token}} when token == job.token ->
        Process.demonitor(ref, [:flush])

        case result do
          :ok -> Outbox.finish(job)
          {:defer, reason} -> Outbox.defer(job, reason)
          error -> Outbox.fail(job, error)
        end

        Process.delete(:last_drain_at)
        send(self(), :tick)
        {:noreply, %{state | active: Map.delete(state.active, pid)}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.get(state.active, pid) do
      {^ref, job} ->
        Outbox.fail(job, {:worker_exit, reason})
        send(self(), :tick)
        {:noreply, %{state | active: Map.delete(state.active, pid)}}

      _ ->
        {:noreply, state}
    end
  end

  defp drain(state) do
    now = System.monotonic_time(:millisecond)
    previous = Process.get(:last_renew_at)

    if is_nil(previous) or now - previous >= 30_000 do
      Enum.each(state.active, fn {_pid, {_ref, job}} -> Outbox.renew(job) end)
      Outbox.recover_expired()
      Process.put(:last_renew_at, now)
    end

    cap =
      case Integer.parse(System.get_env("BORS_ASYNC_WORKERS", "2")) do
        {n, ""} -> min(max(n, 1), 4)
        _ -> 2
      end

    # Bound returned work and exclude occupied repositories before selection.
    # The joined priority query may still sort a larger eligible backlog.
    Enum.reduce(Outbox.due(32), state, fn job, state ->
      if map_size(state.active) < cap do
        case Outbox.claim(job.id) do
          {:ok, nil} ->
            state

          {:ok, claimed} ->
            {:ok, pid} = Worker.get(claimed.project_id)
            ref = Process.monitor(pid)
            send(pid, {:execute, self(), claimed})
            %{state | active: Map.put(state.active, pid, {ref, claimed})}
        end
      else
        state
      end
    end)
  end
end
