defmodule BorsNG.Worker.Orchestration.Outbox do
  @moduledoc """
  Durable, bounded scheduler operations. Claims and completion are fenced with
  random tokens; restart never replays an operation while its lease is live.
  Repository lanes serialize staging and main promotion, including all branches.
  """
  import Ecto.Query
  alias BorsNG.Database.{OrchestrationJob, Project, Repo}

  def enabled?, do: System.get_env("BORS_ASYNC_ORCHESTRATION", "false") == "true"
  def now, do: System.system_time(:second)

  def enqueue(project_id, kind, payload, key \\ nil, available_at \\ now()) do
    {kind, subject} =
      case {kind, payload} do
        {"cast", {:cancel_generation, id, _, _, _}} -> {"cancel", id}
        {"cast", {:cancel, id, _}} -> {"cancel", id}
        {"cast", {:cancel, id}} -> {"cancel", id}
        {"cast", {:cancel_all}} -> {"cancel_all", nil}
        _ -> {kind, nil}
      end

    key =
      if is_nil(key) and kind == "cast" and match?({:status, _}, payload),
        do:
          "status:#{project_id}:" <>
            Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(payload))),
        else: key

    job =
      Repo.insert!(
        %OrchestrationJob{
          project_id: project_id,
          kind: kind,
          subject_id: subject,
          payload: :erlang.term_to_binary(payload),
          dedup_key: key,
          available_at: available_at
        },
        on_conflict: :nothing
      )

    if Process.whereis(BorsNG.Worker.Orchestration.Dispatcher),
      do: send(BorsNG.Worker.Orchestration.Dispatcher, :tick)

    job
  end

  def payload(job), do: :erlang.binary_to_term(job.payload, [:safe])

  def claim(id) do
    Repo.transaction(fn ->
      job = Repo.get!(OrchestrationJob, id)
      project = Repo.one(from(p in Project, where: p.id == ^job.project_id, lock: "FOR UPDATE"))

      if is_nil(project) do
        job
        |> Ecto.Changeset.change(state: "done", dedup_key: nil, last_error: "project_deleted")
        |> Repo.update!()

        nil
      else
        job = Repo.one!(from(j in OrchestrationJob, where: j.id == ^id, lock: "FOR UPDATE"))

        # Current locking read is necessary on MySQL: a consistent read
        # could reuse the snapshot from before waiting for the project lock.
        busy =
          Repo.one(
            from(j in OrchestrationJob,
              where:
                j.project_id == ^job.project_id and j.state == "running" and
                  j.lease_until > ^now(),
              select: j.id,
              limit: 1,
              lock: "FOR UPDATE"
            )
          )

        if job.state == "pending" and job.available_at <= now() and is_nil(busy) do
          token = Base.encode16(:crypto.strong_rand_bytes(16))

          job
          |> Ecto.Changeset.change(
            state: "running",
            token: token,
            lease_until: now() + 300,
            attempts: job.attempts + 1
          )
          |> Repo.update!()
        end
      end
    end)
  end

  def renew(job), do: owned(job) |> Repo.update_all(set: [lease_until: now() + 300])

  def finish(job) do
    owned(job)
    |> Repo.update_all(
      set: [
        state: "done",
        dedup_key: if(job.kind in ["notice", "dispatch"], do: job.dedup_key, else: nil),
        token: nil,
        lease_until: 0,
        updated_at: NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
      ]
    )
  end

  def fail(job, reason) do
    delay = min(60 * Integer.pow(2, min(job.attempts - 1, 6)), 3600)
    delay = if match?({:worker_exit, _}, reason), do: max(delay, 300), else: delay

    owned(job)
    |> Repo.update_all(
      set: [
        state: "pending",
        available_at: now() + delay,
        token: nil,
        lease_until: 0,
        last_error: inspect(reason, limit: 8, printable_limit: 200)
      ]
    )
  end

  def withdrawal_pending?(project_id, patch_ids) do
    Repo.exists?(
      from(j in OrchestrationJob,
        where:
          j.project_id == ^project_id and j.state == "pending" and
            (j.kind == "cancel_all" or (j.kind == "cancel" and j.subject_id in ^patch_ids))
      )
    )
  end

  def pending?(project_id, key) do
    Repo.exists?(
      from(j in OrchestrationJob,
        where:
          j.project_id == ^project_id and j.dedup_key == ^key and
            j.state in ["pending", "running"]
      )
    )
  end

  def due(limit) do
    busy =
      from(j in OrchestrationJob,
        where: j.state == "running" and j.lease_until > ^now(),
        select: j.project_id
      )

    Repo.all(
      from(j in OrchestrationJob,
        where:
          j.state == "pending" and j.available_at <= ^now() and j.project_id not in subquery(busy),
        order_by: j.id,
        limit: ^limit
      )
    )
  end

  def recover_expired do
    Repo.update_all(
      from(j in OrchestrationJob, where: j.state == "running" and j.lease_until <= ^now()),
      set: [state: "pending", token: nil, lease_until: 0]
    )
  end

  def owned?(job), do: Repo.exists?(owned(job))

  defp owned(job),
    do:
      from(j in OrchestrationJob,
        where: j.id == ^job.id and j.state == "running" and j.token == ^job.token
      )
end
