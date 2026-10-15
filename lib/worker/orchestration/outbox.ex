defmodule BorsNG.Worker.Orchestration.Outbox do
  @moduledoc """
  Durable, bounded scheduler operations. Claims and completion are fenced with
  random tokens; restart never replays an operation while its lease is live.
  Repository lanes serialize staging and main promotion, including all branches.
  """
  require Logger
  import Ecto.Query
  alias BorsNG.Database.{Batch, LinkPatchBatch, OrchestrationJob, Patch, Project, Repo}

  @notifications ["notice", "projection"]
  def notification?(job), do: job.kind in @notifications

  def enabled?, do: System.get_env("BORS_ASYNC_ORCHESTRATION", "false") == "true"
  def now, do: System.system_time(:second)

  def enqueue(project_id, kind, payload, key \\ nil, available_at \\ now()) do
    {kind, subject} =
      case {kind, payload} do
        {"cast", {:cancel_generation, id, _, _, _}} -> {"cancel", id}
        {"cast", {:cancel, id, _}} -> {"cancel", id}
        {"cast", {:cancel, id}} -> {"cancel", id}
        {"cast", {:cancel_all_generation, _}} -> {"cancel_all", nil}
        {"cast", {:cancel_all}} -> {"cancel_all", nil}
        _ -> {kind, nil}
      end

    attributes = schedule_attributes(project_id, kind, payload)
    # A read before a future timer's deadline cannot satisfy that timer.
    wake_key = if available_at <= now(), do: wake_key(project_id, kind, payload)

    key =
      if is_nil(key) and kind == "cast" and match?({:status, _}, payload),
        do:
          "status:#{project_id}:" <>
            Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(payload))),
        else: key

    job =
      %OrchestrationJob{
        project_id: project_id,
        kind: kind,
        subject_id: subject || attributes.subject_id,
        priority: attributes.priority,
        enqueued_at_ms: System.system_time(:millisecond),
        payload: :erlang.term_to_binary(payload),
        dedup_key: wake_key || key,
        available_at: available_at
      }
      |> insert_job(wake_key)

    if Process.whereis(BorsNG.Worker.Orchestration.Dispatcher),
      do: send(BorsNG.Worker.Orchestration.Dispatcher, :tick)

    job
  end

  # These requests read current authoritative state; their payload is a wakeup,
  # never a verdict or an approval/cancellation command.
  defp wake_key(project, "cast", {:eligibility, pr}), do: "wake:#{project}:eligibility:#{pr}"
  defp wake_key(project, "info", {:poll, :once}), do: "wake:#{project}:poll"

  defp wake_key(project, "info", operation)
       when operation in [:recover_queue, :wake_backend_holds],
       do: "wake:#{project}:#{operation}"

  defp wake_key(_, _, _), do: nil

  defp insert_job(job, nil),
    do: with_project(job.project_id, fn -> Repo.insert!(job, on_conflict: :nothing) end)

  defp insert_job(job, key) do
    {:ok, result} =
      Repo.transaction(fn ->
        # Match claim's lock order. MySQL's unique-key insert plus a locking
        # read can otherwise deadlock simultaneous producers and completion.
        Repo.one(from(p in Project, where: p.id == ^job.project_id, lock: "FOR UPDATE"))
        Repo.insert!(job, on_conflict: :nothing)

        current =
          Repo.one(from(j in OrchestrationJob, where: j.dedup_key == ^key, lock: "FOR UPDATE"))

        cond do
          is_nil(current) ->
            nil

          current.state == "pending" and command_after?(current) ->
            # Keep the earlier read, but a later wakeup must also observe commands
            # queued in between. Give that later read a new FIFO position.
            current |> Ecto.Changeset.change(dedup_key: nil) |> Repo.update!()
            job |> Ecto.Changeset.change(wake_version: 1) |> Repo.insert!()

          true ->
            changes =
              if current.state == "running" do
                [
                  rerun_at: min(current.rerun_at || job.available_at, job.available_at),
                  rerun_enqueued_at_ms:
                    min(current.rerun_enqueued_at_ms || job.enqueued_at_ms, job.enqueued_at_ms)
                ]
              else
                ready =
                  if current.attempts > 0 and current.last_error,
                    do: current.available_at,
                    else: min(current.available_at, job.available_at)

                [available_at: ready]
              end

            current
            |> Ecto.Changeset.change(
              changes ++
                [
                  wake_version: current.wake_version + 1,
                  subject_id: job.subject_id,
                  priority: job.priority
                ]
            )
            |> Repo.update!()
        end
      end)

    # Concurrent completion may clear the key after ON CONFLICT. The receipt
    # must acquire a fresh owner instead of being acknowledged without work.
    if result, do: result, else: insert_job(job, key)
  end

  defp command_after?(job) do
    # Current locking read: MySQL may otherwise reuse a pre-lock snapshot.
    not is_nil(
      Repo.one(
        from(j in OrchestrationJob,
          where:
            j.project_id == ^job.project_id and j.id > ^job.id and
              j.state in ["pending", "running"] and j.wake_version == 0 and
              (not is_nil(j.subject_id) or j.kind == "cancel_all"),
          select: j.id,
          order_by: [desc: j.id],
          limit: 1,
          lock: "FOR UPDATE"
        )
      )
    )
  end

  def payload(job), do: :erlang.binary_to_term(job.payload, [:safe])

  # All observations of a running candidate share one FIFO class, including
  # pending/red observations. Never move an old green ahead of a newer red.
  def schedule_attributes(project_id, kind, payload, database \\ Repo) do
    subject =
      case payload do
        {:eligibility, pr} ->
          database.one(
            from(p in Patch,
              where: p.project_id == ^project_id and p.pr_xref == ^pr,
              select: p.id
            )
          ) || 0

        command when kind == "cast" and is_tuple(command) and tuple_size(command) >= 2 ->
          if is_integer(elem(command, 1)), do: elem(command, 1)

        _ ->
          nil
      end

    priority =
      case {kind, payload} do
        {"dispatch", _} ->
          1

        {kind, _} when kind in ["cancel", "cancel_all"] ->
          0

        {"cast", {:status, {commit, _, _, _}}} ->
          if database.exists?(
               from(b in Batch,
                 where: b.project_id == ^project_id and b.commit == ^commit
               )
             ),
             do: 1,
             else: 2

        {"cast", _} ->
          if subject == 0, do: 0, else: 2

        {"info", {:poll, _}} ->
          1

        {"info", {:completion_poll, _}} ->
          1

        {"info", {:retry_promotion, _, _}} ->
          1

        {"info", :restore_rechecks} ->
          1

        _ ->
          2
      end

    %{priority: priority, subject_id: subject}
  end

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
        is_notification = notification?(job)

        busy_query =
          from(j in OrchestrationJob,
            where:
              j.project_id == ^job.project_id and j.state == "running" and j.lease_until > ^now(),
            select: j.id,
            limit: 1,
            lock: "FOR UPDATE"
          )

        busy_query =
          if is_notification,
            do: from(j in busy_query, where: j.kind in ^@notifications),
            else: from(j in busy_query, where: j.kind not in ^@notifications)

        busy = Repo.one(busy_query)

        older =
          if is_notification do
            Repo.one(
              from(j in OrchestrationJob,
                where:
                  j.project_id == ^job.project_id and j.id < ^job.id and
                    j.kind in ^@notifications and j.state in ["pending", "running"],
                select: j.id,
                limit: 1,
                lock: "FOR UPDATE"
              )
            )
          end

        if job.state == "pending" and job.available_at <= now() and is_nil(busy) and is_nil(older) do
          token = Base.encode16(:crypto.strong_rand_bytes(16))

          # A command received before construction can become relevant while
          # queued. Upgrade it under the same lane lock before accounting fairness.
          member_state =
            if job.kind == "cast" and job.subject_id && job.subject_id != 0 do
              Repo.one(
                from(l in LinkPatchBatch,
                  join: b in Batch,
                  on: b.id == l.batch_id,
                  where:
                    b.project_id == ^job.project_id and b.state in [:waiting, :running] and
                      l.patch_id == ^job.subject_id,
                  order_by: [desc: b.state],
                  select: b.state,
                  limit: 1,
                  lock: "FOR UPDATE"
                )
              )
            end

          priority =
            case member_state do
              :running -> 0
              :waiting -> 1
              _ -> job.priority
            end

          # Fairness survives dispatcher/executor restarts and retries. One
          # background operation gets a turn after at most eight critical claims.
          streak =
            cond do
              is_notification -> project.orchestration_streak
              priority < 2 -> min(project.orchestration_streak + 1, 8)
              true -> 0
            end

          project |> Ecto.Changeset.change(orchestration_streak: streak) |> Repo.update!()

          job
          |> Ecto.Changeset.change(
            claimed_version: job.wake_version,
            rerun_at: nil,
            rerun_enqueued_at_ms: nil,
            claimed_at_ms: System.system_time(:millisecond),
            state: "running",
            token: token,
            lease_until: now() + 300,
            attempts: job.attempts + 1
          )
          |> Repo.update!()
          |> Map.put(:priority, priority)
        end
      end
    end)
  end

  defp with_project(project_id, action) do
    {:ok, result} =
      Repo.transaction(fn ->
        Repo.one(from(p in Project, where: p.id == ^project_id, lock: "FOR UPDATE"))
        action.()
      end)

    result
  end

  def renew(job),
    do:
      with_project(job.project_id, fn ->
        owned(job) |> Repo.update_all(set: [lease_until: now() + 300])
      end)

  def finish(%{wake_version: version} = job) when version > 0 do
    {:ok, result} =
      Repo.transaction(fn ->
        Repo.one(from(p in Project, where: p.id == ^job.project_id, lock: "FOR UPDATE"))
        current = Repo.one(from(j in owned(job), lock: "FOR UPDATE"))

        if current do
          unless is_integer(current.claimed_version), do: Repo.rollback(:missing_wake_claim)
          dirty = current.wake_version > current.claimed_version
          key = current.dedup_key

          current
          |> Ecto.Changeset.change(
            state: "done",
            dedup_key: nil,
            wake_version: current.claimed_version,
            completed_at_ms: System.system_time(:millisecond),
            token: nil,
            lease_until: 0
          )
          |> Repo.update!()

          if dirty do
            attributes = schedule_attributes(current.project_id, current.kind, payload(current))
            # A continuation belongs after commands queued during the read. Row
            # replacement and key transfer commit atomically with completion.
            Repo.insert!(%OrchestrationJob{
              project_id: current.project_id,
              kind: current.kind,
              subject_id: attributes.subject_id,
              priority: attributes.priority,
              payload: current.payload,
              dedup_key: key,
              available_at: current.rerun_at || now(),
              enqueued_at_ms: current.rerun_enqueued_at_ms || System.system_time(:millisecond),
              wake_version: current.wake_version - current.claimed_version
            })
          end

          {1, nil}
        else
          {0, nil}
        end
      end)

    if elem(result, 0) > 0 and job.subject_id, do: wake_deferred(job.project_id)
    result
  end

  def finish(job) do
    result =
      with_project(job.project_id, fn ->
        owned(job)
        |> Repo.update_all(
          set: [
            state: "done",
            completed_at_ms: System.system_time(:millisecond),
            dedup_key:
              if(job.kind in ["notice", "projection", "dispatch"], do: job.dedup_key, else: nil),
            token: nil,
            lease_until: 0,
            updated_at: NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
          ]
        )
      end)

    if elem(result, 0) > 0 and (job.subject_id || job.kind == "cancel_all"),
      do: wake_deferred(job.project_id)

    result
  end

  defp wake_deferred(project_id) do
    with_project(project_id, fn ->
      Repo.update_all(
        from(j in OrchestrationJob,
          where:
            j.project_id == ^project_id and j.state == "pending" and
              j.last_error == "deferred:withdrawal_pending"
        ),
        set: [available_at: now()]
      )
    end)
  end

  def defer(job, reason) do
    with_project(job.project_id, fn ->
      pending_at =
        Repo.one(
          from(j in OrchestrationJob,
            where:
              j.project_id == ^job.project_id and j.state == "pending" and
                (j.kind == "cancel_all" or not is_nil(j.subject_id)),
            select: min(j.available_at)
          )
        )

      owned(job)
      |> Repo.update_all(
        set: [
          state: "pending",
          token: nil,
          lease_until: 0,
          available_at: max(now() + 1, pending_at || 0),
          attempts: max(job.attempts - 1, 0),
          completed_at_ms: System.system_time(:millisecond),
          last_error: "deferred:#{reason}"
        ]
      )
    end)
  end

  def fail(job, reason) do
    if notification?(job) and (job.attempts >= 6 or match?({:terminal, _}, reason)) do
      Logger.warning(
        "outbox_notification_terminal job=#{job.id} project=#{job.project_id} attempts=#{job.attempts} reason=#{inspect(reason, limit: 8)}"
      )

      with_project(job.project_id, fn ->
        owned(job)
        |> Repo.update_all(
          set: [
            state: "done",
            token: nil,
            lease_until: 0,
            completed_at_ms: System.system_time(:millisecond),
            last_error:
              "notification_terminal:" <> inspect(reason, limit: 8, printable_limit: 160)
          ]
        )
      end)
    else
      retry(job, reason)
    end
  end

  defp retry(job, reason) do
    if job.attempts >= 6,
      do:
        Logger.warning(
          "outbox_retry_stuck job=#{job.id} project=#{job.project_id} kind=#{job.kind} attempts=#{job.attempts}"
        )

    delay = min(60 * Integer.pow(2, min(job.attempts - 1, 6)), 3600)

    delay =
      if match?({:worker_exit, _}, reason) or job.kind in ["notice", "projection"],
        do: max(delay, 300),
        else: delay

    with_project(job.project_id, fn ->
      owned(job)
      |> Repo.update_all(
        set: [
          state: "pending",
          completed_at_ms: System.system_time(:millisecond),
          available_at: now() + delay,
          token: nil,
          lease_until: 0,
          last_error: inspect(reason, limit: 8, printable_limit: 200)
        ]
      )
    end)
  end

  def withdrawal_pending?(project_id, patch_ids) do
    Repo.exists?(
      from(j in OrchestrationJob,
        where:
          j.project_id == ^project_id and j.state == "pending" and
            (j.kind == "cancel_all" or
               (j.kind in ["cancel", "cast"] and
                  (j.subject_id in ^patch_ids or j.subject_id == 0)))
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

  defp active_members do
    from(l in LinkPatchBatch,
      join: b in Batch,
      on: b.id == l.batch_id,
      where: b.state in [:waiting, :running],
      group_by: [b.project_id, l.patch_id],
      select: %{
        project_id: b.project_id,
        patch_id: l.patch_id,
        rank: min(fragment("CASE WHEN ? = 1 THEN 0 ELSE 1 END", b.state))
      }
    )
  end

  def due(limit, lane \\ :mutation)

  def due(limit, :notification) do
    heads =
      from(j in OrchestrationJob,
        where: j.kind in ^@notifications and j.state in ["pending", "running"],
        group_by: j.project_id,
        select: %{project_id: j.project_id, id: min(j.id)}
      )

    Repo.all(
      from(j in OrchestrationJob,
        join: h in subquery(heads),
        on: h.id == j.id,
        where: j.state == "pending" and j.available_at <= ^now(),
        order_by: j.id,
        limit: ^limit
      )
    )
  end

  def due(limit, :mutation) do
    busy =
      from(j in OrchestrationJob,
        where: j.state == "running" and j.lease_until > ^now() and j.kind not in ^@notifications,
        select: j.project_id
      )

    Repo.all(
      from(j in OrchestrationJob,
        left_join: p in Project,
        on: p.id == j.project_id,
        left_join: a in subquery(active_members()),
        on: a.project_id == j.project_id and a.patch_id == j.subject_id and j.kind == "cast",
        where:
          j.state == "pending" and j.available_at <= ^now() and j.kind not in ^@notifications and
            j.project_id not in subquery(busy),
        order_by: [
          asc:
            fragment(
              "CASE WHEN ? >= 8 AND ? = 2 AND ? IS NULL THEN -1 WHEN ? IS NOT NULL THEN ? ELSE ? END",
              p.orchestration_streak,
              j.priority,
              a.patch_id,
              a.patch_id,
              a.rank,
              j.priority
            ),
          asc: j.id
        ],
        limit: ^limit
      )
    )
  end

  def recover_expired do
    projects =
      Repo.all(
        from(j in OrchestrationJob,
          where: j.state == "running" and j.lease_until <= ^now(),
          distinct: true,
          select: j.project_id,
          limit: 32
        )
      )

    count =
      Enum.reduce(projects, 0, fn project_id, count ->
        {changed, _} =
          with_project(project_id, fn ->
            Repo.update_all(
              from(j in OrchestrationJob,
                where:
                  j.project_id == ^project_id and j.state == "running" and j.lease_until <= ^now()
              ),
              set: [state: "pending", token: nil, lease_until: 0]
            )
          end)

        count + changed
      end)

    {count, nil}
  end

  def owned?(job), do: Repo.exists?(owned(job))

  defp owned(job),
    do:
      from(j in OrchestrationJob,
        where: j.id == ^job.id and j.state == "running" and j.token == ^job.token
      )
end
