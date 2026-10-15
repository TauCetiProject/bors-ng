defmodule BorsNG.Worker.Orchestration.Worker do
  @moduledoc "Repository executor retaining legacy timer/approval state between durable operations."
  use GenServer, restart: :temporary
  require Logger
  alias BorsNG.Worker.Orchestration.Outbox
  alias BorsNG.Worker.Batcher

  def start_link(project_id),
    do:
      GenServer.start_link(__MODULE__, project_id,
        name: {:via, Registry, {BorsNG.Worker.Orchestration.Registry, project_id}}
      )

  def get(project_id) do
    case Registry.lookup(BorsNG.Worker.Orchestration.Registry, project_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        DynamicSupervisor.start_child(
          BorsNG.Worker.Orchestration.Workers,
          {__MODULE__, project_id}
        )
    end
  end

  def init(project_id) do
    Process.put(:orchestration_executor, true)
    send(self(), :restore_rechecks)
    send(self(), :recover_backend_holds)
    {:ok, project_id}
  end

  def handle_info({:execute, dispatcher, job}, project_id) do
    Process.put(:orchestration_job, job)
    started = System.monotonic_time(:millisecond)
    inserted = job.inserted_at |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix()

    ready_wait_ms =
      max((job.claimed_at || Outbox.now()) - max(inserted, job.available_at), 0) * 1000

    Logger.info(
      "outbox_start job=#{job.id} project=#{project_id} kind=#{job.kind} priority=#{job.priority} attempt=#{job.attempts} ready_wait_ms=#{ready_wait_ms}"
    )

    # An executing job may not outlive its durable lease. Killing this process
    # cancels its local work; replay reconciles an already accepted API mutation.
    executor = self()

    killer =
      spawn_link(fn ->
        receive do
          :done -> :ok
        after
          1_100_000 -> Process.exit(executor, :kill)
        end
      end)

    result =
      try do
        if Outbox.owned?(job) do
          case {job.kind, Outbox.payload(job)} do
            {kind, args} when kind in ["cast", "cancel", "cancel_all"] ->
              Batcher.handle_cast(args, project_id)

            {"info", args} ->
              Batcher.handle_info(args, project_id)

            {"dispatch", {batch_id, publisher}} ->
              Batcher.resume_dispatch(batch_id, publisher)

            {"dispatch", batch_id} ->
              Batcher.resume_dispatch(batch_id)

            {"notice", {conn, pr, body, marker}} ->
              deliver_notice(job, conn, pr, body, marker)
          end
        end

        :ok
      rescue
        error -> {:error, error.__struct__}
      catch
        kind, _reason -> {:error, kind}
      end

    service_ms = System.monotonic_time(:millisecond) - started
    measurements = %{ready_wait_ms: ready_wait_ms, service_ms: service_ms}

    metadata = %{
      job_id: job.id,
      project_id: project_id,
      kind: job.kind,
      priority: job.priority,
      result: result
    }

    :telemetry.execute([:bors, :outbox, :operation], measurements, metadata)

    Logger.info(
      "outbox_finish job=#{job.id} project=#{project_id} kind=#{job.kind} priority=#{job.priority} ready_wait_ms=#{ready_wait_ms} service_ms=#{service_ms} result=#{inspect(result)}"
    )

    if service_ms > 10_000,
      do:
        Logger.warning(
          "outbox_service_budget_exceeded job=#{job.id} kind=#{job.kind} service_ms=#{service_ms} budget_ms=10000"
        )

    send(killer, :done)
    Process.delete(:orchestration_job)
    send(dispatcher, {:finished, self(), job, result})
    {:noreply, project_id}
  end

  def handle_info(args, project_id) do
    Outbox.enqueue(project_id, "info", args, timer_key(project_id, args))
    {:noreply, project_id}
  end

  defp deliver_notice(job, conn, pr, body, marker) do
    seen =
      if job.attempts > 1 do
        since =
          job.inserted_at
          |> DateTime.from_naive!("Etc/UTC")
          |> DateTime.add(-60, :second)
          |> DateTime.to_iso8601()

        {:ok, comments} = BorsNG.GitHub.notice_comments_since(conn, pr, since)

        cond do
          Enum.any?(comments, &String.contains?(&1, marker)) -> true
          length(comments) >= 100 -> throw(:comment_lookup_truncated)
          true -> false
        end
      else
        false
      end

    unless seen, do: :ok = BorsNG.GitHub.post_comment_once(conn, pr, body)
  end

  defp timer_key(project_id, {:poll, _}), do: "poll:#{project_id}"
  defp timer_key(_, _), do: nil
end
