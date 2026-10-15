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

    # Separate bounded notification capacity cannot consume a mutation slot.
    state = launch(Outbox.due(32, :mutation), state, cap, false)
    launch(Outbox.due(32, :notification), state, 1, true)
  end

  defp launch(jobs, state, cap, notification) do
    Enum.reduce(jobs, state, fn job, state ->
      active =
        Enum.count(state.active, fn {_pid, {_ref, running}} ->
          Outbox.notification?(running) == notification
        end)

      occupied =
        Enum.any?(state.active, fn {_pid, {_ref, running}} ->
          running.project_id == job.project_id and
            Outbox.notification?(running) == notification
        end)

      # due/2 can return many rows for a repository that was free when read.
      # Once launched, skip its remaining rows in this turn without taking
      # redundant project/job locks. claim/1 remains the durable lease fence.
      if active < cap and not occupied do
        case Outbox.claim(job.id) do
          {:ok, nil} ->
            state

          {:ok, claimed} ->
            key =
              if notification, do: {:notification, claimed.project_id}, else: claimed.project_id

            {:ok, pid} = Worker.get(key)
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
