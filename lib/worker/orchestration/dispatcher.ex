defmodule BorsNG.Worker.Orchestration.Dispatcher do
  use GenServer
  require Logger
  alias BorsNG.Worker.Orchestration.{Outbox, Worker}

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def init(_) do
    Process.flag(:trap_exit, true)
    {:ok, %{active: %{}}, {:continue, :start}}
  end

  # The supervisor stops us before the executors and the database. Give active
  # commands a chance to finish, without claiming more work. Never release an
  # interrupted command: a shared HTTP client or Git child may still be working.
  # Keep this below our supervisor's five-second shutdown budget.
  def terminate(reason, state) when reason in [:normal, :shutdown] do
    started = System.monotonic_time(:millisecond)
    Logger.info("outbox_shutdown_started active=#{map_size(state.active)} budget_ms=4000")
    drain_shutdown(state.active, started + 4_000)

    Logger.info(
      "outbox_shutdown_finished drain_ms=#{System.monotonic_time(:millisecond) - started}"
    )
  end

  def terminate({:shutdown, _}, state), do: terminate(:shutdown, state)
  def terminate(_, _), do: :ok

  defp drain_shutdown(active, _deadline) when map_size(active) == 0, do: :ok

  defp drain_shutdown(active, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      retain(active)
    else
      await_shutdown(active, deadline, remaining)
    end
  end

  defp await_shutdown(active, deadline, remaining) do
    receive do
      {:finished, pid, job, result} ->
        case Map.get(active, pid) do
          {ref, %{token: token} = claimed} when token == job.token ->
            # Use our claim, not the message's other fields. The command has
            # returned before this message; no external request remains in it.
            Outbox.record_shutdown_result(claimed)
            complete(claimed, result)
            Process.demonitor(ref, [:flush])
            Logger.info("outbox_shutdown_completed job=#{claimed.id} attempt=#{claimed.attempts}")
            drain_shutdown(Map.delete(active, pid), deadline)

          _ ->
            drain_shutdown(active, deadline)
        end

      {:DOWN, ref, :process, pid, _reason} ->
        case Map.get(active, pid) do
          {^ref, job} ->
            log_retained(job)
            drain_shutdown(Map.delete(active, pid), deadline)

          _ ->
            drain_shutdown(active, deadline)
        end
    after
      remaining -> retain(active)
    end
  end

  defp retain(active), do: Enum.each(active, fn {_pid, {_ref, job}} -> log_retained(job) end)

  defp log_retained(job) do
    Logger.info(
      "outbox_shutdown_retained job=#{job.id} project=#{job.project_id} attempt=#{job.attempts} claimed_at_ms=#{job.claimed_at_ms} lease_until=#{job.lease_until}"
    )
  end

  defp complete(job, result) do
    case result do
      :ok -> Outbox.finish(job)
      {:defer, reason} -> Outbox.defer(job, reason)
      error -> Outbox.fail(job, error)
    end
  end

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
      {ref, %{token: token} = claimed} when token == job.token ->
        Process.demonitor(ref, [:flush])
        complete(claimed, result)

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
