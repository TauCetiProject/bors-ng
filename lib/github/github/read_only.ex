defmodule BorsNG.GitHub.ReadOnly do
  @moduledoc """
  Independent read requests with the normal HTTP retry and token-refresh policy.

  The coordinator owns only the installation-token cache. HTTP work runs in monitored tasks,
  so time spent behind another batch cannot expire a read. Each HTTP request still has the
  configured transport timeout; there is no deadline for collecting an entire batch.
  """
  use GenServer
  alias BorsNG.GitHub

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def request(type, conn, args)
      when type in [:batch_read, :public_user, :get_pr, :get_pr_commits, :get_pr_identity] do
    GenServer.call(__MODULE__, {type, conn, args}, :infinity)
  catch
    :exit, _ -> {:error, :read_client_unavailable}
  end

  def init(_), do: {:ok, %{tokens: %{}, pending: %{}, callers: %{}}}

  def handle_call(request, from, state) do
    cached = state.tokens

    task =
      Task.Supervisor.async_nolink(BorsNG.GitHub.ReadTasks, fn ->
        try do
          Process.put(:github_read_only, true)

          if Confex.fetch_env!(:bors, :server) == GitHub.Server do
            {:reply, result, tokens} = GitHub.Server.handle_call(request, nil, cached)
            # Do not write unchanged snapshot entries back over concurrent refreshes.
            {result, Map.reject(tokens, fn {id, value} -> cached[id] == value end)}
          else
            # Tests retain the deterministic mock's shared state.
            {GenServer.call(GitHub, request, :infinity), %{}}
          end
        rescue
          error in Tesla.Error -> {{:error, :read_transport, error.reason}, %{}}
          error -> {{:error, :read_exception, error.__struct__}, %{}}
        catch
          kind, _ -> {{:error, :read_exception, kind}, %{}}
        end
      end)

    caller = Process.monitor(elem(from, 0))
    pending = Map.put(state.pending, task.ref, %{from: from, caller: caller, pid: task.pid})
    {:noreply, %{state | pending: pending, callers: Map.put(state.callers, caller, task.ref)}}
  end

  def handle_info({ref, {result, tokens}}, state) do
    Process.demonitor(ref, [:flush])
    {entry, pending} = Map.pop(state.pending, ref)

    callers =
      if entry do
        Process.demonitor(entry.caller, [:flush])
        GenServer.reply(entry.from, result)
        Map.delete(state.callers, entry.caller)
      else
        state.callers
      end

    {:noreply,
     %{
       state
       | pending: pending,
         callers: callers,
         tokens:
           Map.merge(state.tokens, tokens, fn _id, old, new ->
             if elem(new, 1) >= elem(old, 1), do: new, else: old
           end)
     }}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.pending, ref) do
      {nil, _} ->
        case Map.pop(state.callers, ref) do
          {nil, _} ->
            {:noreply, state}

          {task_ref, callers} ->
            {entry, pending} = Map.pop(state.pending, task_ref)
            Process.exit(entry.pid, :kill)
            Process.demonitor(task_ref, [:flush])
            {:noreply, %{state | pending: pending, callers: callers}}
        end

      {entry, pending} ->
        Process.demonitor(entry.caller, [:flush])
        GenServer.reply(entry.from, {:error, :read_worker_unavailable})
        {:noreply, %{state | pending: pending, callers: Map.delete(state.callers, entry.caller)}}
    end
  end
end
