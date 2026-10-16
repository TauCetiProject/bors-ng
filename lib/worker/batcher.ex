require Logger

defmodule BorsNG.Worker.Batcher do
  @moduledoc """
  A "Batcher" manages the backlog of batches a project has.
  It implements this set of rules:

    * When a patch is reviewed ("r+'ed"),
      it gets added to the project's non-running batch.
      If no such batch exists, it creates it.
    * After a short delay, if there is no currently running batch,
      the project's non-running batch is started.
    * The project's CI is occasionally polled,
      if a batch is currently running.
      After polling, the completion logic is run.
    * If a notification related to the underlying CI is received,
      the completion logic is run.
    * When the completion logic is run, the batch is either
      bisected (if it failed and there are two or more patches in it),
      blocked (if it failed and there is only one patch in it),
      pushed to master (if it passed),
      or (if there are still CI jobs with no results) it is left alone.
  """

  use GenServer
  alias BorsNG.Database.Context.Delegation
  alias BorsNG.Worker.Batcher
  alias BorsNG.Worker.Batcher.Bundles
  alias BorsNG.Worker.Batcher.Divider
  alias BorsNG.Worker.Labeler
  alias BorsNG.Worker.Syncer
  alias BorsNG.Worker.QueueApproval
  alias BorsNG.Worker.Zulip
  alias BorsNG.Database.Repo
  alias BorsNG.Database.Batch
  alias BorsNG.Database.Patch
  alias BorsNG.Database.Project
  alias BorsNG.Database.Status
  alias BorsNG.Database.LinkPatchBatch
  alias BorsNG.Worker.Orchestration.Outbox
  alias BorsNG.Worker.CandidateJournal
  alias BorsNG.Worker.SpeculativeTrain, as: Train
  alias BorsNG.GitHub
  alias BorsNG.Endpoint
  import BorsNG.Router.Helpers
  import Ecto.Query

  @prerun_poll_period 1 * 60 * 1000

  @type batch_state :: :waiting | :running | :ok | :error | :conflict | :canceled

  # Public API

  def start_link(project_id) do
    GenServer.start_link(__MODULE__, project_id)
  end

  def eligibility(pid, pr) when is_integer(pr), do: submit(pid, {:eligibility, pr})

  def reviewed(pid, patch_id, reviewer) when is_integer(patch_id) do
    submit(pid, {:reviewed, patch_id, reviewer})
  end

  # The legacy path casts because bundle activation may hold the batcher.
  # Async callers wait only for the durable mailbox insert, never GitHub work.
  def set_is_single(pid, patch_id, is_single) when is_integer(patch_id) do
    submit(pid, {:set_is_single, patch_id, is_single})
  end

  def set_priority(pid, patch_id, priority) when is_integer(patch_id) do
    submit(pid, {:set_priority, patch_id, priority})
  end

  def status(pid, stat) do
    submit(pid, {:status, stat})
  end

  def poll(pid) do
    if Outbox.enabled?(),
      do: GenServer.call(pid, {:persist_info, {:poll, :once}}),
      else: send(pid, {:poll, :once})
  end

  def cancel(pid, patch_id, reason \\ :requested) when is_integer(patch_id) do
    submit(pid, {:cancel, patch_id, reason})
  end

  def cancel_all(pid) do
    submit(pid, {:cancel_all})
  end

  def link(pid, patch_id, pr_xrefs) when is_integer(patch_id) do
    submit(pid, {:link, patch_id, pr_xrefs})
  end

  def stack(pid, patch_id, pr_xrefs) when is_integer(patch_id) do
    submit(pid, {:stack, patch_id, pr_xrefs})
  end

  def unlink(pid, patch_id) when is_integer(patch_id) do
    submit(pid, {:unlink, patch_id})
  end

  @doc "Persist an async command before acknowledging its webhook caller."
  def submit(pid, args) do
    if Outbox.enabled?(),
      do: GenServer.call(pid, {:persist, args}),
      else: GenServer.cast(pid, args)
  end

  # Allocate intent generations at receipt, atomically with their durable
  # mailbox insert. Cancellation priority must never reverse r+ / r- / r+.
  defp persist_command(project_id, {:cancel_all}) do
    {:ok, _} =
      Repo.transaction(fn ->
        patches =
          Repo.all(
            from(p in Patch,
              where: p.project_id == ^project_id and p.open,
              order_by: p.id,
              lock: "FOR UPDATE"
            )
          )

        token = Base.encode16(:crypto.strong_rand_bytes(16))

        Repo.update_all(from(p in Patch, where: p.id in ^Enum.map(patches, & &1.id)),
          set: [orchestration_approval_token: token]
        )

        Outbox.enqueue(
          project_id,
          "cast",
          {:cancel_all_generation, Enum.map(patches, &{&1.id, &1.commit, token})}
        )
      end)
  end

  defp persist_command(project_id, args) do
    {:ok, _} =
      Repo.transaction(fn ->
        id =
          case args do
            {:reviewed, id, _} -> id
            {:cancel, id, _} -> id
            {:cancel, id} -> id
            {:maintainer_reviewed, id, _, _, _, _} -> id
            _ -> nil
          end

        patch =
          if id,
            do:
              Repo.one(
                from(p in Patch,
                  where: p.id == ^id and p.project_id == ^project_id,
                  lock: "FOR UPDATE"
                )
              )

        if patch do
          token =
            if match?({:cancel, _, :review_unsafe}, args),
              do: patch.orchestration_approval_token,
              else: Base.encode16(:crypto.strong_rand_bytes(16))

          patch |> Patch.changeset(%{orchestration_approval_token: token}) |> Repo.update!()

          fenced =
            case args do
              {:reviewed, _, reviewer} ->
                {:reviewed_generation, id, patch.commit, token, reviewer}

              {:cancel, _, reason} ->
                {:cancel_generation, id, patch.commit, token, reason}

              {:cancel, _} ->
                {:cancel_generation, id, patch.commit, token, :requested}

              {:maintainer_reviewed, _, head, actor, reviewer, time} ->
                {:maintainer_reviewed_generation, id, head, token, actor, reviewer, time}
            end

          Outbox.enqueue(project_id, "cast", fenced)
        else
          if is_nil(id), do: Outbox.enqueue(project_id, "cast", args)
        end
      end)
  end

  defp with_intent(patch_id, generation, action) do
    previous = Process.get(:intake_generation)
    Process.put(:intake_generation, {patch_id, generation})

    try do
      action.()
    after
      if previous,
        do: Process.put(:intake_generation, previous),
        else: Process.delete(:intake_generation)
    end
  end

  defp intent_current?(patch) do
    case Process.get(:intake_generation) do
      {id, generation} when id == patch.id ->
        Repo.get!(Patch, id).orchestration_approval_token == generation

      _ ->
        not Process.get(:orchestration_executor, false) or
          Repo.get!(Patch, patch.id).orchestration_approval_token ==
            patch.orchestration_approval_token
    end
  end

  # Server callbacks

  def init(project_id) do
    BorsNG.Worker.Batcher.Registry.monitor(self(), project_id)

    schedule_message(
      self(),
      {:poll, :repeat},
      trunc(Confex.fetch_env!(:bors, :poll_period) * :rand.uniform(2) * 0.5)
    )

    # Timers do not survive a process restart; their deadlines do. Re-arm
    # existing running holds rather than waiting for the coarse normal poll.
    send(self(), :restore_rechecks)

    state = if Outbox.enabled?(), do: %{async_project_id: project_id}, else: project_id
    {:ok, state}
  end

  def handle_cast(args, %{async_project_id: project_id} = state) do
    persist_command(project_id, args)
    {:noreply, state}
  end

  def handle_cast(args, project_id) do
    check_self(project_id)
    do_handle_cast(args, project_id)
    {:noreply, project_id}
  end

  # Retain direct priority/single calls already in flight across a deploy.
  def handle_call({:persist, args}, _from, %{async_project_id: project_id} = state) do
    persist_command(project_id, args)
    {:reply, :ok, state}
  end

  def handle_call({:persist_info, args}, _from, %{async_project_id: project_id} = state) do
    Outbox.enqueue(project_id, "info", args, "poll:#{project_id}")
    {:reply, :ok, state}
  end

  def handle_call(args, _from, %{async_project_id: project_id} = state) do
    persist_command(project_id, args)
    {:reply, :ok, state}
  end

  def handle_call({:set_is_single, _, _} = args, _from, project_id) do
    do_handle_cast(args, project_id)
    {:reply, :ok, project_id}
  end

  def handle_call({:set_priority, _, _} = args, _from, project_id) do
    do_handle_cast(args, project_id)
    {:reply, :ok, project_id}
  end

  def do_handle_cast({:set_is_single, patch_id, is_single}, _project_id) do
    case Repo.get(Patch, patch_id) do
      nil ->
        nil

      %{is_single: ^is_single} ->
        nil

      patch ->
        # Bundled patches queue as a unit, so the `single` setting applies
        # to all members, not just one.
        patch
        |> Bundles.members_or_self()
        |> Enum.each(fn p ->
          p
          |> Patch.changeset(%{is_single: is_single})
          |> Repo.update!()
        end)
    end
  end

  def do_handle_cast({:set_priority, patch_id, priority}, project_id) do
    case Repo.get(Patch, patch_id) do
      nil ->
        nil

      %{priority: ^priority} ->
        nil

      patch ->
        patch.id
        |> Batch.all_for_patch(:incomplete)
        |> Repo.one()
        |> raise_batch_priority(priority)

        # Bundled patches enter the queue at one priority level. A priority
        # change applies to all members.
        patch
        |> Bundles.members_or_self()
        |> Enum.each(fn p ->
          p
          |> Patch.changeset(%{priority: priority})
          |> Repo.update!()
        end)

        Project.ping!(project_id)
    end
  end

  def do_handle_cast({:eligibility, pr}, project_id) do
    BorsNG.Worker.MergeEligibility.reconcile(Repo.get!(Project, project_id), pr)
  end

  def do_handle_cast(
        {:maintainer_reviewed_generation, id, head, generation, actor, reviewer, time},
        project_id
      ) do
    case Repo.get(Patch, id) do
      %Patch{project_id: ^project_id, commit: ^head, orchestration_approval_token: ^generation} ->
        with_intent(id, generation, fn ->
          do_handle_cast({:maintainer_reviewed, id, head, actor, reviewer, time}, project_id)
        end)

      _ ->
        :ok
    end
  end

  def do_handle_cast(
        {:maintainer_reviewed, patch_id, head, actor_id, reviewer, created_at},
        project_id
      ) do
    project = Repo.get!(Project, project_id)
    patch = Repo.get!(Patch, patch_id)
    conn = get_repo_conn(project)

    if patch.project_id == project_id do
      case BorsNG.Worker.ManualApproval.grant(conn, project, patch, actor_id, head, created_at) do
        {:ok, approved} ->
          send_message(
            conn,
            [approved],
            {:manual_approved, head, approved.manual_approval["merge_base_sha"]}
          )

          # This is a request to reconsider policy, never a green scope status.
          # The trusted workflow reads the approval back from bors and still runs all guards.
          GitHub.reset_scope(conn, head)

          if GitHub.dispatch_pr_build(conn, approved.pr_xref) != :ok,
            do: send_message(conn, [approved], {:preflight, :manual_build_dispatch})

          do_handle_cast({:reviewed, patch_id, reviewer}, project_id)

        :waiting ->
          send_message(conn, [patch], {:preflight, :manual_approval_unavailable})
          do_handle_cast({:reviewed, patch_id, reviewer}, project_id)

        _ ->
          send_message(conn, [patch], {:preflight, :manual_approval_changed})
      end
    end
  end

  def do_handle_cast({:reviewed_generation, patch_id, head, generation, reviewer}, project_id) do
    case Repo.get(Patch, patch_id) do
      %Patch{project_id: ^project_id, commit: ^head, orchestration_approval_token: ^generation} ->
        with_intent(patch_id, generation, fn ->
          do_handle_cast({:reviewed_expected, patch_id, head, reviewer}, project_id)
        end)

      _ ->
        :ok
    end
  end

  def do_handle_cast({:reviewed, patch_id, reviewer}, project_id) do
    case Repo.get(Patch, patch_id) do
      nil -> :ok
      patch -> do_handle_cast({:reviewed_expected, patch_id, patch.commit, reviewer}, project_id)
    end
  end

  def do_handle_cast({:reviewed_expected, patch_id, expected_head, reviewer}, _project_id) do
    case Repo.get(Patch.all(:awaiting_review), patch_id) do
      nil ->
        # Patch exists (otherwise, no ID), but is not awaiting review
        patch = Repo.get!(Patch, patch_id)
        project = Repo.get!(Project, patch.project_id)

        project
        |> get_repo_conn()
        |> send_message([patch], :already_running_review)

      %Patch{commit: head} when head != expected_head ->
        :ok

      patch ->
        # Patch exists and is awaiting review
        patch = QueueApproval.remember(patch, reviewer)
        # This will cause the PR to run after the patch's scheduled delay
        # if all other conditions are met. It will poll if all conditions
        # except CI are met and those CI are :waiting.
        project = Repo.get!(Project, patch.project_id)
        repo_conn = get_repo_conn(project)

        case patch_preflight(repo_conn, patch) do
          {:ok, max_batch_size} ->
            activate(reviewer, patch, max_batch_size)

          {:waiting, toml} ->
            handle_waiting_preflight(
              repo_conn,
              hold_prerun_approval(project, patch, reviewer),
              patch,
              0,
              toml
            )

          :waiting ->
            handle_waiting_preflight(
              repo_conn,
              hold_prerun_approval(project, patch, reviewer),
              patch,
              0
            )

          {:error, message} ->
            QueueApproval.stop([patch], :preflight)
            send_message(repo_conn, [patch], {:preflight, message})
        end
    end
  end

  def do_handle_cast({:recover_reviewed, patch_id, expected_head, reviewer}, project_id) do
    case Repo.get(Patch.all(:awaiting_review), patch_id) do
      %Patch{commit: ^expected_head, open: true, is_draft: false} ->
        unless Process.get({:approval_pending, patch_id}),
          do: do_handle_cast({:reviewed_expected, patch_id, expected_head, reviewer}, project_id)

      _ ->
        :ok
    end
  end

  def do_handle_cast({:status, {commit, identifier, state, url}}, project_id) do
    project_id
    |> Batch.get_assoc_by_commit(commit, :incomplete)
    |> Repo.all()
    |> Enum.each(fn batch ->
      previous = Repo.one(Status.get_for_batch(batch.id, identifier))

      {updated, _} =
        if Process.get(:orchestration_executor, false) and previous && previous.state == state &&
             previous.url == url do
          {0, nil}
        else
          batch.id
          |> Status.get_for_batch(identifier)
          |> Repo.update_all(set: [state: state, url: url, identifier: identifier])
        end

      if updated > 0 and batch.state == :running do
        CandidateJournal.status(batch, identifier, state, "github.status_webhook", url)

        if (Process.get(:orchestration_executor, false) and state in [:ok, :error]) or
             (expired_staging?(batch) and
                (batch.recovery_after || 0) <= DateTime.to_unix(DateTime.utc_now()) and
                (batch.promotion_retry_at || 0) <= DateTime.to_unix(DateTime.utc_now())) do
          # A delayed event may omit a newer successful verdict. Read the
          # current head before releasing its staging attempt at the deadline.
          poll_running_batch(batch)
        else
          maybe_complete_batch(batch)
        end
      end
    end)
  end

  def do_handle_cast({:cancel_all_generation, members}, project_id) do
    Enum.each(members, fn {id, head, generation} ->
      do_handle_cast({:cancel_generation, id, head, generation, :cancel_all}, project_id)
    end)
  end

  def do_handle_cast({:cancel_generation, patch_id, head, generation, reason}, project_id) do
    case Repo.get(Patch, patch_id) do
      %Patch{project_id: ^project_id, commit: ^head, orchestration_approval_token: ^generation} ->
        do_handle_cast({:cancel, patch_id, reason}, project_id)

      _ ->
        :ok
    end
  end

  def do_handle_cast({:cancel, patch_id, :review_unsafe}, project_id) do
    patch = Repo.get!(Patch, patch_id)
    project = Repo.get!(Project, patch.project_id)

    if patch.manual_approval &&
         BorsNG.Worker.ManualApproval.preflight(get_repo_conn(project), patch) in [:ok, :waiting] do
      :ok
    else
      do_handle_cast({:cancel, patch_id, :automatic_review_rejected}, project_id)
    end
  end

  def do_handle_cast({:cancel, patch_id, reason}, _project_id) do
    # A bundled patch may hold an approval while its siblings await approval.
    # Canceling (r-, close, or new push) revokes it. Re-queueing the bundle
    # requires a fresh r+ on this member.
    BorsNG.Worker.ManualApproval.revoke(patch_id)
    Bundles.drop_held_approval(patch_id)

    case Repo.get(Patch, patch_id) do
      nil -> :ok
      patch -> QueueApproval.stop(Bundles.members_or_self(patch), reason)
    end

    Process.delete({:approval_pending, patch_id})

    patch_id
    |> Batch.all_for_patch(:incomplete)
    |> Repo.one()
    |> cancel_patch(patch_id, reason)
  end

  # Backwards compatibility for any in-flight 2-tuple casts (e.g. across a deploy).
  def do_handle_cast({:cancel, patch_id}, project_id) do
    do_handle_cast({:cancel, patch_id, :requested}, project_id)
  end

  def do_handle_cast({:cancel_all}, project_id) do
    Repo.all(
      from(p in Patch, where: p.project_id == ^project_id and not is_nil(p.manual_approval))
    )
    |> Enum.each(&BorsNG.Worker.ManualApproval.revoke(&1.id))

    pending = Repo.all(from(p in Patch, where: p.project_id == ^project_id and p.open))
    QueueApproval.stop(pending, :cancel_all)

    waiting =
      project_id
      |> Batch.all_for_project(:waiting)
      |> Repo.all()

    running =
      project_id
      |> Batch.all_for_project(:running)
      |> Repo.all()

    # Capture each batch's patches (with its base branch) before mutating —
    # deleting a waiting batch removes its patch links — so the labels can be
    # reconciled off the queue at the end.
    affected =
      Enum.map(waiting ++ running, fn b ->
        {b.into_branch, b.id |> Patch.all_for_batch() |> Repo.all()}
      end)

    # An emptied queue needs fresh review everywhere. A solo patch's approval
    # dies with its batch links; a bundle member's held approval must die
    # with them too, or one member's later r+ would silently re-queue its
    # whole bundle.
    affected
    |> Enum.flat_map(fn {_branch, patches} -> patches end)
    |> Bundles.drop_held_approvals()

    Enum.each(waiting, &Repo.delete!/1)

    Enum.map(running, &Batch.changeset(&1, %{state: :canceled}))
    |> Enum.each(&Repo.update!/1)

    Enum.each(running, &send_zulip(&1, :canceled))

    repo_conn =
      Project
      |> Repo.get!(project_id)
      |> get_repo_conn()

    Enum.each(running, &send_status(repo_conn, &1, :canceled))
    Enum.each(waiting, &send_status(repo_conn, &1, :canceled))

    Enum.each(affected, fn {branch, patches} ->
      Labeler.reconcile_queue(repo_conn, branch, patches)
    end)
  end

  def do_handle_cast({:link, patch_id, pr_xrefs}, project_id) do
    patch = Repo.get!(Patch, patch_id)
    project = Repo.get!(Project, project_id)
    repo_conn = get_repo_conn(project)

    case Bundles.validate_link(patch, pr_xrefs, project_id) do
      {:error, reason} ->
        send_message(repo_conn, [patch], {:link_error, reason})

      {:ok, members} ->
        members = Bundles.form(members, project_id)
        xrefs = members |> Enum.map(& &1.pr_xref) |> Enum.sort()
        send_message(repo_conn, members, {:linked, xrefs})
    end
  end

  def do_handle_cast({:stack, patch_id, pr_xrefs}, project_id) do
    patch = Repo.get!(Patch, patch_id)
    project = Repo.get!(Project, project_id)
    repo_conn = get_repo_conn(project)

    target_xrefs =
      pr_xrefs
      |> Enum.uniq()
      |> Enum.reject(&(&1 == patch.pr_xref))

    case {pr_xrefs, target_xrefs} do
      {[], []} ->
        # Bare `bors stack`: infer the parent from the base branch chain.
        # A stacked PR's base branch is its parent's head branch.
        case Bundles.infer_stack_parent(patch, project_id) do
          {:ok, parent} ->
            do_stack(repo_conn, patch, parent.pr_xref, project)

          :error ->
            send_message(repo_conn, [patch], {:link_error, :cannot_infer})
        end

      {_, []} ->
        # Only this pull request's own number was given: refuse. This prevents
        # a typo from being read as the bare, inferring form.
        send_message(repo_conn, [patch], {:link_error, :self_stack})

      {_, [target_xref]} ->
        do_stack(repo_conn, patch, target_xref, project)

      _ ->
        send_message(repo_conn, [patch], {:link_error, :stack_usage})
    end
  end

  def do_handle_cast({:unlink, patch_id}, project_id) do
    patch = Repo.get!(Patch, patch_id)

    case patch.bundle_id do
      nil ->
        :ok

      bundle_id ->
        project = Repo.get!(Project, project_id)
        repo_conn = get_repo_conn(project)

        # Read the members before dissolving. Dissolve clears the
        # retargeted_from records that base restoration needs.
        members = Bundles.members(bundle_id)

        case Bundles.dissolve(bundle_id) do
          {:error, :in_batch} ->
            send_message(repo_conn, [patch], {:link_error, :in_batch})

          {:ok, _} ->
            # A member that held an approval for the bundle loses it during
            # dissolve. Unlike its siblings, it will not merge without a fresh r+.
            {held, rest} = Enum.split_with(members, &(&1.bundle_reviewer != nil))
            send_message(repo_conn, rest, :unlinked)
            send_message(repo_conn, held, {:unlinked, :fresh_approval_needed})
            restore_retargeted_bases(repo_conn, members)
        end
    end
  end

  def handle_info(args, %{async_project_id: project_id} = state) do
    {args, key} =
      case args do
        {:poll, repetition} ->
          if repetition != :once,
            do:
              schedule_message(
                self(),
                {:poll, repetition},
                Confex.fetch_env!(:bors, :poll_period)
              )

          {{:poll, :once}, "poll:#{project_id}"}

        _ ->
          {args, nil}
      end

    Outbox.enqueue(project_id, "info", args, key)
    {:noreply, state}
  end

  def handle_info(:recover_backend_holds, project_id) do
    if Process.get(:backend_holds_recovered) do
      {:noreply, project_id}
    else
      Process.put(:backend_holds_recovered, true)
      recover_backend_holds(project_id)
    end
  end

  def handle_info(:wake_backend_holds, project_id), do: recover_backend_holds(project_id)

  def handle_info({:post_merge_cleanup, id, commit, candidate_id, squash, _members}, project_id) do
    if batch = cleanup_batch(project_id, id, commit, candidate_id) do
      squash = if is_nil(squash), do: legacy_cleanup_squash(batch), else: squash

      if squash do
        source = Process.get(:orchestration_job)
        expand_cleanup(source, 0, project_id)
      end
    end

    {:noreply, project_id}
  end

  def handle_info({:expand_post_merge_cleanup, source_id, offset}, project_id) do
    case Repo.get(BorsNG.Database.OrchestrationJob, source_id) do
      %{project_id: ^project_id, kind: "info"} = source ->
        expand_cleanup(source, offset, project_id)

      _ ->
        {:noreply, project_id}
    end
  end

  def handle_info({:close_merged_pr, id, commit, candidate_id, member}, project_id) do
    if batch = cleanup_batch(project_id, id, commit, candidate_id),
      do: close_merged_pr(batch, member)

    {:noreply, project_id}
  end

  def handle_info({:completion_poll, _batch_id}, project_id),
    do: handle_info({:poll, :once}, project_id)

  def handle_info({:poll, repetition}, project_id) do
    check_self(project_id)

    if repetition != :once do
      schedule_message(self(), {:poll, repetition}, Confex.fetch_env!(:bors, :poll_period))
    end

    case poll_(project_id) do
      :stop ->
        {:stop, :normal, project_id}

      :again ->
        {:noreply, project_id}
    end
  end

  def handle_info(:recover_queue, project_id), do: recover_queue(project_id, 4)

  def handle_info({:recover_queue_slice, budget, seen}, project_id) when budget in 1..3,
    do: recover_queue(project_id, budget, seen)

  def handle_info({:backend_holds_slice, budget, seen}, project_id) when budget in 1..3,
    do: recover_backend_holds(project_id, budget, seen)

  def handle_info(:restore_rechecks, project_id) do
    try do
      project_id
      |> Batch.all_for_project(:running)
      |> Repo.all()
      |> Enum.each(fn batch ->
        deadline = max(batch.recovery_after || 0, batch.promotion_retry_at || 0)
        if deadline > 0 and is_binary(batch.commit), do: schedule_recheck(batch, deadline)
      end)
    rescue
      error ->
        Logger.warning("batch retry timer restoration deferred: #{inspect(error.__struct__)}")
        schedule_message(self(), :restore_rechecks, 60_000)
    end

    {:noreply, project_id}
  end

  def handle_info({:retry_promotion, batch_id, head}, project_id) do
    cancel_recheck(batch_id)

    case Repo.get(Batch, batch_id) do
      %Batch{project_id: ^project_id, state: :running, commit: ^head} = batch ->
        deadline = max(batch.recovery_after || 0, batch.promotion_retry_at || 0)

        if deadline > DateTime.to_unix(DateTime.utc_now()) do
          schedule_recheck(batch, deadline)
        else
          Process.delete({:promotion_retry, batch_id})
          poll_running_batch(Repo.preload(batch, :project), true)
        end

      _ ->
        :ok
    end

    {:noreply, project_id}
  end

  def handle_info({:backend_retry, patch_id, head, reviewer, generation}, project_id) do
    current = Repo.get(Patch, patch_id)

    valid =
      if Process.get(:orchestration_executor, false),
        do:
          current && current.commit == head && current.orchestration_approval_token == generation &&
            current.bundle_reviewer == reviewer,
        else: Process.get({:approval_pending, patch_id}) == {head, generation}

    if valid do
      handle_info({:backend_retry, patch_id, head, reviewer}, project_id)
    else
      {:noreply, project_id}
    end
  end

  def handle_info({:backend_retry, patch_id, head, reviewer}, project_id) do
    case Repo.get(Patch.all(:awaiting_review), patch_id) do
      %Patch{commit: ^head, bundle_reviewer: ^reviewer, open: true, is_draft: false} ->
        do_handle_cast({:reviewed, patch_id, reviewer}, project_id)

      _ ->
        :ok
    end

    {:noreply, project_id}
  end

  def handle_info({:prerun_poll, try_num, args}, proj_id) do
    check_self(proj_id)
    {reviewer, patch} = args
    approved_head = patch.commit

    Logger.info("Continue Poll Patch #{patch.id} prerun")

    project = Repo.get(Project, patch.project_id)
    repo_conn = get_repo_conn(project)

    case Repo.get(Patch.all(:awaiting_review), patch.id) do
      nil ->
        send_message(repo_conn, [patch], {:preflight, :duplicate})
        Logger.info("Patch #{patch.id} already left prerun, exiting prerun poll loop")

      # The captured patch struct is stale: it may have been bundled,
      # retargeted, or pushed to since the poll was scheduled. Activate using
      # the current row, not the snapshot.
      %Patch{commit: ^approved_head} = patch ->
        case resolve_prerun_reviewer(reviewer, patch) do
          nil ->
            Logger.info("Approval revoked or superseded; discarding delayed preflight")

          resolved ->
            case patch_preflight(repo_conn, patch) do
              {:ok, max_batch_size} ->
                activate(resolved, patch, max_batch_size)

              {:waiting, toml} ->
                handle_waiting_preflight(repo_conn, reviewer, patch, try_num, toml)

              :waiting ->
                handle_waiting_preflight(repo_conn, reviewer, patch, try_num)

              {:error, message} ->
                QueueApproval.stop([patch], :preflight)
                Bundles.drop_held_approval(patch.id)
                send_message(repo_conn, [patch], {:preflight, message})
            end
        end

      _ ->
        Logger.info("Patch head moved; discarding delayed approval")
    end

    {:noreply, proj_id}
  end

  defp recover_queue(project_id, budget, seen \\ []) do
    project = Repo.get!(Project, project_id)
    conn = get_repo_conn(project)
    cursor = Process.get(:queue_recovery_cursor, 0)

    candidates =
      if BorsNG.MergeBackend.allow(project, "main", :admit) == :ok,
        do:
          QueueApproval.candidates(
            project_id,
            cursor,
            if(Process.get(:orchestration_executor, false), do: length(seen) + 1, else: budget)
          ),
        else: []

    # Cursor queries wrap; a short queue must not repeat a PR in one wakeup.
    candidates =
      candidates
      |> Enum.reject(&(&1.id in seen))
      |> Enum.take(if(Process.get(:orchestration_executor, false), do: 1, else: budget))

    Enum.each(candidates, fn patch ->
      Process.put(:queue_recovery_cursor, patch.id)

      try do
        case patch.queue_approval do
          %{"head_sha" => head, "reviewer" => reviewer, "state" => "approved"}
          when head == patch.commit and is_binary(reviewer) ->
            # Cached database state discovers orphans; normal live admission
            # verifies authorization, head, holds and CI before any build starts.
            case GitHub.get_pr(conn, patch.pr_xref) do
              {:ok, pr} ->
                if pr.state == :open and not pr.draft and pr.head_sha == head and
                     pr.base_ref == patch.into_branch do
                  with {:ok, labels} <- GitHub.get_labels(conn, patch.pr_xref),
                       true <-
                         MapSet.disjoint?(
                           MapSet.new(labels, &String.downcase/1),
                           MapSet.new(~w(keep hold wip human do-not-close))
                         ) do
                    do_handle_cast({:recover_reviewed, patch.id, head, reviewer}, project_id)
                    Logger.info("queue_recovered pr=#{patch.pr_xref} head=#{head}")
                  else
                    _ -> :ok
                  end
                else
                  do_handle_cast({:cancel, patch.id, :eligibility_changed}, project_id)
                  if pr.user, do: Syncer.sync_patch(project.id, pr)
                end

              _ ->
                :ok
            end

          %{"head_sha" => head, "state" => "withdrawn", "reason" => reason}
          when head == patch.commit and reason in ["preflight", "preflight_timeout"] ->
            # A failed/cancelled standalone check stops admission, not the
            # exact-head human scope decision. Recover only once all live
            # admission guards pass; red checks cause no writes or comments.
            now = System.monotonic_time(:second)
            key = {:scope_recovery_after, patch.id}

            if not match?({^head, due} when due > now, Process.get(key)) do
              Process.put(key, {head, now + 300})

              admission =
                with {:ok, labels} <- GitHub.get_labels(conn, patch.pr_xref),
                     true <-
                       MapSet.disjoint?(
                         MapSet.new(labels, &String.downcase/1),
                         MapSet.new(~w(keep hold wip human do-not-close))
                       ) do
                  patch_preflight(conn, patch)
                else
                  _ -> :waiting
                end

              case admission do
                {:ok, _} ->
                  actor = get_in(patch.manual_approval || %{}, ["actor_id"])

                  user =
                    if is_integer(actor), do: Repo.get_by(BorsNG.Database.User, user_xref: actor)

                  if user && is_binary(user.login) do
                    # No live held approval or active membership remains.
                    Process.delete({:approval_pending, patch.id})
                    do_handle_cast({:recover_reviewed, patch.id, head, user.login}, project_id)
                  end

                {:error, :manual_approval_changed} ->
                  do_handle_cast({:cancel, patch.id, :eligibility_changed}, project_id)

                _ ->
                  :ok
              end
            end

          _ ->
            QueueApproval.stop([patch], :revision_changed)
        end
      rescue
        error ->
          Logger.warning(
            "queue recovery deferred pr=#{patch.pr_xref}: #{Exception.message(error)}"
          )
      end
    end)

    if candidates != [], do: Process.put(:queue_recovery_cursor, List.last(candidates).id)

    if candidates != [] and budget > 1 and Process.get(:orchestration_executor, false),
      do:
        schedule_message(
          self(),
          {:recover_queue_slice, budget - 1, Enum.map(candidates, & &1.id) ++ seen},
          0
        )

    {:noreply, project_id}
  end

  defp recover_backend_holds(project_id, budget \\ 4, seen \\ []) do
    project = Repo.get!(Project, project_id)

    if BorsNG.MergeBackend.scoped?(project, "main") and
         BorsNG.MergeBackend.allow(project, "main", :admit) == :ok do
      held =
        Repo.all(
          from(p in Patch.all(:awaiting_review),
            where:
              p.project_id == ^project_id and
                p.into_branch == "main" and not is_nil(p.bundle_reviewer)
          )
        )

      cursor = Process.get(:held_recovery_cursor, 0)
      held = Enum.sort_by(held, & &1.id)
      {older, newer} = Enum.split_while(held, &(&1.id <= cursor))

      selected =
        Enum.take(
          Enum.reject(newer ++ older, &(&1.id in seen)),
          if(Process.get(:orchestration_executor, false), do: 1, else: budget)
        )

      Enum.each(selected, fn p ->
        Process.put(:held_recovery_cursor, p.id)

        unless match?({head, _} when head == p.commit, Process.get({:approval_pending, p.id})) do
          Process.put({:approval_pending, p.id}, {p.commit, make_ref()})
          do_handle_cast({:reviewed, p.id, p.bundle_reviewer}, project_id)
        end
      end)

      if selected != [] and budget > 1 and Process.get(:orchestration_executor, false),
        do:
          schedule_message(
            self(),
            {:backend_holds_slice, budget - 1, Enum.map(selected, & &1.id) ++ seen},
            0
          )
    end

    {:noreply, project_id}
  end

  # Private implementation details

  defp poll_(project_id) do
    project = Repo.get(Project, project_id)

    release_unstarted_batches(project)
    BorsNG.Worker.BumpRecovery.prioritize_waiting(project)

    incomplete =
      project_id
      |> Batch.all_for_project(:incomplete)
      |> Repo.all()

    incomplete
    |> Enum.map(&%Batch{&1 | project: project})
    |> sort_batches()
    |> poll_batches()

    held =
      Repo.exists?(
        from(p in Patch,
          where: p.project_id == ^project_id and p.open and not is_nil(p.bundle_reviewer)
        )
      )

    if Enum.empty?(incomplete) and not held do
      :stop
    else
      :again
    end
  end

  defp run(reviewer, patch, max_batch_size) do
    project = Repo.get!(Project, patch.project_id)
    repo_conn = get_repo_conn(project)

    {batch, is_new_batch} =
      get_new_batch(
        max_batch_size,
        patch.project_id,
        patch.into_branch,
        patch.priority,
        patch.is_single
      )

    %LinkPatchBatch{}
    |> LinkPatchBatch.changeset(%{
      batch_id: batch.id,
      patch_id: patch.id,
      reviewer: reviewer,
      head_sha: patch.commit,
      manual_approval: patch.manual_approval
    })
    |> CandidateJournal.insert_member()

    if patch.bundle_id == nil, do: Bundles.drop_held_approval(patch.id)

    # The patch is now on the queue (a :waiting batch); reflect that in the
    # labels. This also clears any lingering `awaiting-requeue` from a previous
    # failed build, since the PR has been put back on the queue.
    Labeler.reconcile_queue(repo_conn, patch.into_branch, [patch])

    Project.ping!(project.id)

    if is_new_batch do
      put_incomplete_on_hold(repo_conn, batch)
    end

    poll_after_delay(project)
    # Maybe not needed because bors adds a commit referencing this.
    # send_message(repo_conn, [patch], {:preflight, :ok})
    send_status(repo_conn, batch.id, [patch], :waiting)
  end

  # A reviewed patch that passed its own preflight enters the queue here. An
  # unbundled patch is queued directly (run/3, the upstream path); a bundled
  # one holds its approval on the patch row until every member of the bundle
  # is approved, then all members enter the same batch together (run_bundle/3).
  defp activate(reviewer, patch, max_batch_size) do
    if intent_current?(patch), do: activate_current(reviewer, patch, max_batch_size)
  end

  defp activate_current(reviewer, patch, max_batch_size) do
    project = Repo.get!(Project, patch.project_id)

    case BorsNG.MergeBackend.allow(project, patch.into_branch, :admit) do
      :ok ->
        activate_allowed(reviewer, patch, max_batch_size)

      {:defer, reason} ->
        Logger.info(
          "merge_backend deferred pr=#{patch.pr_xref} head=#{patch.commit} reason=#{reason}"
        )

        defer_approval(patch, reviewer)
    end
  end

  defp activate_allowed(reviewer, patch, max_batch_size) do
    case patch.bundle_id do
      nil ->
        run(reviewer, patch, max_batch_size)

      bundle_id ->
        patch = Bundles.hold_approval(patch, reviewer)
        activate_bundle(patch, bundle_id, max_batch_size)
    end
  end

  # Bundle activation makes O(members) GitHub calls inside the batcher
  # process — a staleness compare per stack edge, a get/update per retargeted
  # member, and a full preflight per member, each with retry backoff — so a
  # large bundle blocks this project's batcher for the duration. Accepted for
  # now: realistic bundles are small, and the messages that arrive meanwhile
  # are casts that queue up. If it ever hurts, move the read-only checks
  # (stale_stack_pair, bundle_preflight) into a supervised task, and when it
  # reports clear, re-validate membership/approvals against the current rows
  # here before retargeting and queueing — the GitHub writes must stay
  # serialized in this process so an unlink cannot interleave with them.
  defp activate_bundle(patch, bundle_id, max_batch_size) do
    project = Repo.get!(Project, patch.project_id)
    repo_conn = get_repo_conn(project)

    members = Bundles.members(bundle_id)
    pending = Bundles.unapproved(members)

    cond do
      pending != [] ->
        announce_bundle_waiting(repo_conn, patch, pending)

      (stale = Bundles.stale_stack_pair(repo_conn, members)) != nil ->
        # A stacked branch has become stale: the parent was amended after
        # stacking, or a webhook was missed. Hold the bundle instead of
        # merging stale content. Held approvals survive. A rebase clears
        # approval via the push webhook; a fresh r+ re-runs this check.
        {child, parent} = stale
        send_message(repo_conn, members, {:stack_stale, child.pr_xref, parent.pr_xref})

      true ->
        # Everything is approved and fresh. Normalize stacked members' bases
        # onto the final branch, then run bundle preflight. The current patch
        # already passed its own preflight.
        case normalize_bundle_bases(repo_conn, members) do
          {:error, :branch_mismatch} ->
            send_message(repo_conn, members, {:link_error, :branch_mismatch})

          {:error, xref} when is_integer(xref) ->
            send_message(repo_conn, members, {:stack_retarget_failed, xref})

          {:ok, members} ->
            activate_bundle_preflight(repo_conn, project, patch, members, max_batch_size)
        end
    end
  end

  # Tell the approver what the bundle awaits. When exactly one member remains,
  # tell that member it alone holds the bundle and what to do about it.
  defp announce_bundle_waiting(repo_conn, patch, pending) do
    others = Enum.reject(pending, &(&1.id == patch.id))

    if others != [] do
      send_message(repo_conn, [patch], {:bundle_waiting, Enum.map(others, & &1.pr_xref)})
    end

    case pending do
      [last] ->
        send_message(repo_conn, [last], {:bundle_last_unapproved, approval_blocker(last)})

      _ ->
        :ok
    end
  end

  defp approval_blocker(%Patch{open: false}), do: :closed
  defp approval_blocker(%Patch{is_draft: true}), do: :draft
  defp approval_blocker(_), do: :awaiting_review

  defp activate_bundle_preflight(repo_conn, project, patch, members, max_batch_size) do
    others = Enum.reject(members, &(&1.id == patch.id))

    case bundle_preflight(repo_conn, others) do
      :ok ->
        run_bundle(project, members, max_batch_size)

      {:waiting, failed, toml} ->
        # That member's statuses are still pending. Its prerun poll re-enters
        # activate/3 once they settle. Held approvals make this check
        # idempotent. The poll carries a marker, not the reviewer name, so it
        # reads the approval as it stands when the poll fires.
        handle_waiting_preflight(repo_conn, :held_approval, failed, 0, toml)

        repo_conn
        |> send_message(
          Enum.reject(members, &(&1.id == failed.id)),
          {:bundle_held, failed.pr_xref}
        )

      {:error, message, failed} ->
        QueueApproval.stop([failed], :preflight)
        Bundles.drop_held_approval(failed.id)
        send_message(repo_conn, [failed], {:preflight, message})

        repo_conn
        |> send_message(
          Enum.reject(members, &(&1.id == failed.id)),
          {:bundle_held, failed.pr_xref}
        )
    end
  end

  # Move every member's base branch to the bundle's final target before
  # queueing. Members based on their parent's branch are retargeted via
  # GitHub API. Fails closed: any API failure holds the bundle. The move is
  # not undone while the bundle exists. Stack edges continue to order any
  # future merge. The original base is recorded so dissolving can restore it.
  defp normalize_bundle_bases(repo_conn, members) do
    case Bundles.final_target(members) do
      {:ok, final} ->
        members
        |> Enum.reduce_while({:ok, []}, fn p, {:ok, acc} ->
          case retarget_patch(repo_conn, p, final) do
            {:ok, p} -> {:cont, {:ok, [p | acc]}}
            :error -> {:halt, {:error, p.pr_xref}}
          end
        end)
        |> case do
          {:ok, acc} -> {:ok, Enum.reverse(acc)}
          error -> error
        end

      {:error, :branch_mismatch} = error ->
        error
    end
  end

  defp retarget_patch(_repo_conn, %Patch{into_branch: final} = patch, final), do: {:ok, patch}

  defp retarget_patch(repo_conn, patch, final) do
    with {:ok, pr} <- GitHub.get_pr(repo_conn, patch.pr_xref),
         {:ok, _} <- GitHub.update_pr_base(repo_conn, %{pr | base_ref: final}) do
      # The "edited" webhook echoes this update. Writing it now keeps
      # activation working with the normalized base. The old base is kept so
      # dissolve can restore it. If the echo outruns this write, the syncer
      # treats it as a base bors did not record and clears bookkeeping. The
      # retarget stands. A later unlink leaves the base as is.
      patch =
        patch
        |> Patch.changeset(%{into_branch: final, retargeted_from: patch.into_branch})
        |> Repo.update!()

      send_message(repo_conn, [patch], {:retargeted, final})
      {:ok, patch}
    else
      _ -> :error
    end
  end

  # Undo queue-time retargeting when a bundle dissolves without merging. A
  # member whose base bors moved—and which still points where bors left it—
  # is retargeted back. This prevents an unlinked child from showing (and
  # squash-merging) its parent's changes. Bases a human has moved since, or
  # bases of closed pull requests, are left alone.
  defp restore_retargeted_bases(repo_conn, members) do
    Enum.each(members, fn patch ->
      cond do
        is_nil(patch.retargeted_from) or patch.retargeted_from == patch.into_branch ->
          :ok

        patch.open ->
          restore_base(repo_conn, patch)

        true ->
          # A closed pull request's base cannot be edited. If it is reopened,
          # its base is still where the bundle left it. Warn about this.
          send_message(repo_conn, [patch], {:base_restore_failed, patch.retargeted_from})
      end
    end)
  end

  defp restore_base(repo_conn, patch) do
    with {:ok, pr} <- GitHub.get_pr(repo_conn, patch.pr_xref),
         %{base_ref: current} when current == patch.into_branch <- pr,
         {:ok, _} <- GitHub.update_pr_base(repo_conn, %{pr | base_ref: patch.retargeted_from}) do
      patch
      |> Patch.changeset(%{into_branch: patch.retargeted_from})
      |> Repo.update!()

      send_message(repo_conn, [patch], {:base_restored, patch.retargeted_from})
    else
      %GitHub.Pr{} ->
        # The base was moved by hand after bors retargeted it. Respect that.
        :ok

      _ ->
        send_message(repo_conn, [patch], {:base_restore_failed, patch.retargeted_from})
    end
  end

  defp bundle_preflight(_repo_conn, []), do: :ok

  defp bundle_preflight(repo_conn, [patch | rest]) do
    case patch_preflight(repo_conn, patch) do
      {:ok, _} -> bundle_preflight(repo_conn, rest)
      :waiting -> {:waiting, patch, nil}
      {:waiting, toml} -> {:waiting, patch, toml}
      {:error, message} -> {:error, message, patch}
    end
  end

  # Bundle equivalent of run/3: the same queueing sequence, N patches at once.
  # Each member's reviewer comes from its held approval. Kept separate so the
  # single-patch path stays untouched. Changes to the sequence in either
  # function must be mirrored in the other.
  defp run_bundle(project, members, max_batch_size) do
    case BorsNG.MergeBackend.allow(project, hd(members).into_branch, :admit) do
      :ok ->
        run_bundle_allowed(project, members, max_batch_size)

      {:defer, _} ->
        Enum.each(members, fn p ->
          defer_approval(p, p.bundle_reviewer)
        end)
    end
  end

  defp run_bundle_allowed(project, members, max_batch_size) do
    repo_conn = get_repo_conn(project)
    members = Bundles.stack_order(members)
    into_branch = hd(members).into_branch

    priority = members |> Enum.map(& &1.priority) |> Enum.max()
    is_single = Enum.any?(members, & &1.is_single)

    {batch, is_new_batch} =
      get_new_batch(
        max_batch_size,
        project.id,
        into_branch,
        priority,
        is_single,
        Enum.count(members)
      )

    # All members join the batch or none do. A crash after a partial insert
    # would leave a batch that merges only part of the bundle.
    {:ok, _} =
      Repo.transaction(fn ->
        Enum.each(members, fn p ->
          %LinkPatchBatch{}
          |> LinkPatchBatch.changeset(%{
            batch_id: batch.id,
            patch_id: p.id,
            reviewer: p.bundle_reviewer,
            head_sha: p.commit,
            manual_approval: p.manual_approval
          })
          |> CandidateJournal.insert_member()
        end)
      end)

    Labeler.reconcile_queue(repo_conn, into_branch, members)

    Project.ping!(project.id)

    if is_new_batch do
      put_incomplete_on_hold(repo_conn, batch)
    end

    poll_after_delay(project)
    send_status(repo_conn, batch.id, members, :waiting)
  end

  defp do_stack(repo_conn, patch, target_xref, project) do
    case Bundles.validate_link(patch, [target_xref], project.id, :stack) do
      {:error, reason} ->
        send_message(repo_conn, [patch], {:link_error, reason})

      {:ok, members} ->
        target = Enum.find(members, &(&1.pr_xref == target_xref))

        # The shape once this edge is recorded: `patch` stops being a root and
        # hangs under `target`. Check target-branch agreement against that
        # shape, so an inconsistent forest is refused here instead of wedging
        # at activation (`final_target`).
        staged =
          Enum.map(members, fn m ->
            if m.id == patch.id, do: %{m | stacked_on_id: target.id}, else: m
          end)

        cond do
          Bundles.creates_stack_cycle?(target, patch) ->
            send_message(repo_conn, [patch], {:link_error, :cycle})

          not Bundles.contains_head?(repo_conn, target, patch) ->
            # If the target contains this patch's head instead, the user most
            # likely commented on the parent. Name the fix.
            message =
              if Bundles.contains_head?(repo_conn, patch, target) do
                {:link_error, {:stack_reversed, target_xref, patch.pr_xref}}
              else
                {:link_error, {:not_rebased, target_xref}}
              end

            send_message(repo_conn, [patch], message)

          match?({:error, _}, Bundles.final_target(staged)) ->
            send_message(repo_conn, [patch], {:link_error, :branch_mismatch})

          true ->
            members = Bundles.form_stacked(members, patch, target, project.id)
            compare_url = compare_url(project, target, patch)
            send_message(repo_conn, members, {:stacked, patch.pr_xref, target_xref, compare_url})
        end
    end
  end

  # A point-in-time compare view of the child's own changes: exactly the delta
  # that contains_head?/3 verified. Built from SHAs, so it works when branches
  # live in a fork.
  defp compare_url(project, parent, child) do
    root = Confex.fetch_env!(:bors, :html_github_root)
    "#{root}/#{project.name}/compare/#{parent.commit}...#{child.commit}"
  end

  @doc """
  Sort incomplete work. Serial running polls order running batches only by the
  operational key; their waiting tail is unused. `:successor` explicitly orders
  that tail by admission age with the pass's single metadata read.
  """
  def sort_batches(batches, purpose \\ :serial) do
    # A serial pass with running work cannot select waiting work. Defer its one
    # metadata read to successor selection, if train eligibility permits it.
    members =
      if purpose == :successor or not Enum.any?(batches, &(&1.state == :running)),
        do: CandidateJournal.queue_age_members(batches),
        else: []

    ages =
      BorsNG.Worker.Batcher.QueueAge.keys(batches, members, System.system_time(:microsecond))

    sorted_batches =
      Enum.sort_by(batches, &BorsNG.Worker.Batcher.QueueAge.order_key(&1, ages))

    new_batches = Enum.dedup_by(sorted_batches, & &1.id)

    state =
      if new_batches != [] and hd(new_batches).state == :running do
        :running
      else
        Enum.each(new_batches, fn batch -> :waiting = batch.state end)
        :waiting
      end

    {state, new_batches}
  end

  defp waiting_eligible?(batch, now) do
    BorsNG.Worker.Batcher.QueueAge.eligible?(batch, now, Batch.next_poll_is_past(batch, now))
  end

  defp poll_batches({:waiting, batches}) do
    now = DateTime.to_unix(DateTime.utc_now())

    case Enum.filter(batches, &waiting_eligible?(&1, now)) do
      [] -> :ok
      [batch | _] -> start_waiting_batch(batch)
    end
  end

  defp poll_batches({:running, batches}) do
    if Process.get(:orchestration_executor, false) do
      Enum.each(Enum.filter(batches, &(&1.state == :running)), fn batch ->
        case Repo.get(Batch, batch.id) do
          %Batch{state: :running} = current -> poll_one_running(Repo.preload(current, :project))
          _ -> :ok
        end
      end)

      maybe_start_successor(hd(batches).project)
    else
      poll_one_running(hd(batches))
    end
  end

  defp maybe_start_successor(project) do
    batches = Repo.all(Batch.all_for_project(project.id, :incomplete))
    conn = get_repo_conn(project)

    if parent = Train.predecessor(project, batches, conn) do
      {_state, ordered} = sort_batches(batches, :successor)

      waiting =
        Enum.find(
          ordered,
          &waiting_eligible?(%{&1 | project: project}, DateTime.to_unix(DateTime.utc_now()))
        )

      if waiting do
        links = Repo.all(LinkPatchBatch.from_batch(waiting.id))

        if Train.links_safe?(links, conn) and
             BorsNG.MergeBackend.allow(project, waiting.into_branch, :start) == :ok and
             waiting.into_branch == parent.into_branch do
          start_waiting_batch_allowed(%{waiting | project: project}, parent)
        end
      end
    end
  end

  defp poll_one_running(batch) do
    cond do
      (batch.recovery_after || 0) > DateTime.to_unix(DateTime.utc_now()) ->
        :ok

      (batch.promotion_retry_at || 0) > DateTime.to_unix(DateTime.utc_now()) ->
        :ok

      Batch.timeout_is_past(batch) and green_staged?(batch) ->
        # CI has finished. A restart during an infrastructure hold must retry
        # promotion of the same staged commit, not classify it as a CI timeout.
        poll_running_batch(batch)

      expired_staging?(batch) ->
        # Refresh even when no terminal status arrived. A missing verdict
        # cannot establish an individual proof failure.
        poll_running_batch(batch)

      Batch.timeout_is_past(batch) ->
        timeout_batch(batch)

      Batch.next_poll_is_past(batch) ->
        poll_running_batch(batch)

      true ->
        :ok
    end
  end

  defp green_staged?(batch) do
    statuses = Repo.all(Status.all_for_batch(batch.id))
    is_binary(batch.commit) and statuses != [] and Enum.all?(statuses, &(&1.state == :ok))
  end

  defp cancel_recheck(batch_id) do
    if timer = Process.delete({:batch_recheck, batch_id}), do: Process.cancel_timer(timer)
  end

  defp schedule_recheck(batch, deadline) do
    cancel_recheck(batch.id)
    delay = max(deadline - DateTime.to_unix(DateTime.utc_now()) + 1, 1)
    timer = schedule_message(self(), {:retry_promotion, batch.id, batch.commit}, delay * 1_000)
    Process.put({:batch_recheck, batch.id}, timer)
  end

  defp staging_dispatch?(batch) do
    is_binary(batch.commit) and
      System.get_env("BORS_STAGE_DISPATCH_PROJECT") == batch.project.name
  end

  defp expired_staging?(batch), do: staging_dispatch?(batch) and Batch.timeout_is_past(batch)

  defp staged_failure?(batch, statuses) do
    staging_dispatch?(batch) and
      Enum.any?(
        statuses,
        &(&1.identifier in ["build", "bump-guard", "scope"] and &1.state == :error)
      )
  end

  defp start_waiting_batch(batch) do
    started =
      Repo.exists?(Status.all_for_batch(batch.id)) or
        (is_nil(batch.commit) and (batch.timeout_at || 0) > 0)

    purpose = if started, do: :drain, else: :start

    case BorsNG.MergeBackend.allow(batch.project, batch.into_branch, purpose) do
      :ok ->
        start_waiting_batch_allowed(batch)

      {:defer, reason} ->
        Logger.info("merge_backend deferred batch=#{batch.id} reason=#{reason}")
        schedule_message(self(), {:poll, :once}, 60_000)
    end
  end

  # This runs inside the project's batcher, serialized with activation and CI
  # dispatch. CI status rows or an unresolved armed start protect builds across
  # restarts; a paused batch with abandoned CI may transfer. Retain archived
  # links and exact-head approvals, so a
  # later switch back to bors recovers intent without replaying consumed checks.
  def release_unstarted_batches(nil), do: :ok

  def release_unstarted_batches(project) do
    if BorsNG.MergeBackend.scoped?(project, "main") do
      waiting =
        Repo.all(
          from(b in Batch.all_for_project(project.id, :waiting),
            left_join: s in Status,
            on: s.batch_id == b.id,
            where:
              b.into_branch == "main" and is_nil(s.id) and
                (is_nil(b.timeout_at) or b.timeout_at == 0 or not is_nil(b.commit))
          )
        )

      if waiting != [] do
        conn = get_repo_conn(project)

        case GitHub.merge_backend_snapshot(conn) do
          {:ok, %{backend: "queue"}} ->
            Enum.each(waiting, fn batch ->
              links = Repo.all(LinkPatchBatch.from_batch(batch.id))

              {:ok, _} =
                Repo.transaction(fn ->
                  batch |> Batch.changeset(%{state: :canceled}) |> Repo.update!()

                  Enum.each(links, fn link ->
                    patch =
                      Repo.one!(
                        from(p in Patch, where: p.id == ^link.patch_id, lock: "FOR UPDATE")
                      )

                    if patch.open and not patch.is_draft and link.head_sha == patch.commit and
                         is_binary(link.head_sha) do
                      Bundles.hold_approval(patch, link.reviewer)
                    end
                  end)
                end)

              Enum.each(links, fn link ->
                patch = Repo.get!(Patch, link.patch_id)

                # Stay dormant while GitHub is selected. The shared heartbeat
                # wakes these holds once bors is selected and GitHub is drained.
                # Do not create one full-preflight retry loop per transferred PR.
                if patch.bundle_reviewer == link.reviewer and patch.commit == link.head_sha,
                  do: Process.delete({:approval_pending, patch.id})
              end)

              Labeler.reconcile_queue(conn, "main", Enum.map(links, & &1.patch))

              Logger.info(
                Jason.encode!(%{
                  schema: "tauceti-merge.handoff/v1",
                  from: "bors",
                  to: "queue",
                  batch_id: batch.id,
                  released_prs: Enum.map(links, & &1.patch.pr_xref)
                })
              )
            end)

          _ ->
            :ok
        end
      end
    end
  end

  defp start_waiting_batch_allowed(batch, predecessor \\ nil) do
    if Process.get(:orchestration_executor, false),
      do:
        ensure_no_withdrawal!(
          batch,
          Repo.all(LinkPatchBatch.from_batch(batch.id)) |> Enum.map(& &1.patch_id)
        )

    # Persist before staging work: a restart or priority preemption may leave
    # this batch waiting again, but its tested membership must never grow.
    batch = batch |> Batch.changeset(%{sealed: true}) |> Repo.update!()
    project = batch.project
    repo_conn = get_repo_conn(project)

    {closed_links, links} =
      Repo.all(LinkPatchBatch.from_batch(batch.id))
      |> Enum.sort_by(& &1.patch.pr_xref)
      |> Enum.split_with(&(&1.patch.open == false))

    # The cheap half of the draft guard. `Command.run/1` refuses `r+` on a
    # draft and converting to draft cancels the batch, but both hang on a
    # webhook bors may never receive. `Syncer.sync_patch/2` writes `is_draft`
    # on every `pull_request` event, so the column costs nothing to read and
    # is fresher than the approval that queued this patch.
    #
    # This does not replace `draft_at_merge?/2`, and neither replaces this
    # one. A PR already drafted when the batch starts is caught here, before
    # it burns a CI run; a PR drafted *during* the build never passes through
    # here again, which is why the merge-time check reads live from GitHub.
    {draft_links, links} = Enum.split_with(links, & &1.patch.is_draft)

    # An ejected patch takes its whole bundle with it. Linked patches merge
    # together or not at all.
    {pulled_by_closed, links} = Bundles.split_pulled_by(closed_links, links)
    {pulled_by_draft, patch_links} = Bundles.split_pulled_by(draft_links, links)

    ejected_links = closed_links ++ draft_links ++ pulled_by_closed ++ pulled_by_draft
    Enum.each(ejected_links, &Repo.delete!/1)
    QueueApproval.stop(Enum.map(ejected_links, & &1.patch), :closed_or_draft)

    closed_patches = Enum.map(closed_links, & &1.patch)
    drafted_patches = Enum.map(draft_links, & &1.patch)

    send_bundle_pulled(repo_conn, Enum.map(pulled_by_closed, & &1.patch), closed_patches, :closed)
    send_bundle_pulled(repo_conn, Enum.map(pulled_by_draft, & &1.patch), drafted_patches, :draft)

    # A closed PR was already told when it closed. A draft has been told
    # nothing, so say why it is not building.
    send_message(repo_conn, drafted_patches, :draft_dropped_before_batch)

    # Approved, off the queue, and needing a human: the same standing as a
    # draft ejected at merge time, so drop what it held and flag it. A draft
    # cannot hold a bundle approval either (`Bundles.hold_approval/2`).
    dropped_by_draft = drafted_patches ++ Enum.map(pulled_by_draft, & &1.patch)
    Bundles.drop_held_approvals(dropped_by_draft)
    Labeler.mark_awaiting_requeue(repo_conn, batch.into_branch, dropped_by_draft)

    # A PR closed or drafted while queued has just left the queue; reconcile
    # its labels off. The links are gone now and these patches are dropped
    # from the reconcile below, so they wouldn't be touched otherwise.
    Labeler.reconcile_queue(
      repo_conn,
      batch.into_branch,
      Enum.map(ejected_links, & &1.patch)
    )

    # Stacked patches merge after the patch they stack on. Bundles merge as
    # contiguous units.
    patch_links = Bundles.sort_links_for_merge(patch_links)

    base =
      cond do
        patch_links == [] ->
          nil

        predecessor ->
          %{
            commit: CandidateJournal.commit_binding(predecessor)["commit"],
            tree: CandidateJournal.commit_binding(predecessor)["tree"]
          }

        true ->
          get_base(repo_conn, batch.into_branch)
      end

    patch_links =
      if patch_links == [],
        do: [],
        else:
          BorsNG.Worker.SourceCompatibility.admit(
            project,
            repo_conn,
            batch,
            patch_links,
            base.commit
          )

    # If all patches were closed and removed, cancel the batch early
    if Enum.empty?(patch_links) do
      now = DateTime.to_unix(DateTime.utc_now(), :second)
      send_status(repo_conn, batch, :canceled)

      batch
      |> Batch.changeset(%{state: :canceled, commit: nil, last_polled: now})
      |> Repo.update!()

      send_zulip(batch, :canceled)
      Project.ping!(project.id)
      :canceled
    else
      candidate =
        CandidateJournal.begin(
          batch,
          patch_links,
          base.commit,
          if(predecessor, do: predecessor.id)
        )

      BorsNG.Worker.SourceCompatibility.journal(candidate, patch_links)
      batch = %{batch | candidate_id: candidate.id}

      mode = BorsNG.Worker.LocalGit.mode(project, batch.into_branch, repo_conn)

      # Async local assembly cannot publish a remote ref before durable binding.
      # Native publication-before-binding retains conservative status routing.
      if mode == "local" and Process.get(:orchestration_executor, false),
        do: CandidateJournal.event(candidate, "local_assembly_selected", "bors.assembly")

      # A rollback may disable local assembly as well as speculation. Retire
      # completed external reservations before deciding whether legacy serial
      # construction may resume; never free slots merely from batch state.
      if Train.reservations(project.id) != [], do: Train.reconcile(project.id, repo_conn)

      controlled =
        Train.enabled?(project, batch.into_branch) or Train.reservations(project.id) != []

      train = controlled and mode == "local" and Process.get(:orchestration_executor, false)
      admitted = if train, do: Train.reserve(candidate, repo_conn), else: {:ok, :ok}

      {status, commit} =
        if (not controlled or train) and (is_nil(predecessor) or train) and admitted == {:ok, :ok} do
          if predecessor,
            do:
              CandidateJournal.event(predecessor, "successor_reserved", "bors.train", %{
                candidate_id: candidate.id
              })

          assemble_candidate_batch(batch, patch_links, base, candidate, mode)
        else
          {:waiting, nil}
        end

      if status == :waiting and mode == "local" do
        Repo.update_all(from(b in Batch, where: b.id == ^batch.id),
          set: [recovery_after: DateTime.to_unix(DateTime.utc_now()) + 300]
        )
      end

      if status != :running do
        CandidateJournal.event(candidate, "construction_stopped", "bors.assembly", %{
          state: status
        })
      end

      now = DateTime.to_unix(DateTime.utc_now(), :second)

      send_status(repo_conn, batch, status)

      case status do
        :running ->
          # when a batch starts set priority to 100
          # so that we can move patches to the "front of the line" without interrupting running batches
          raise_batch_priority(batch, 100)

        _ ->
          :ok
      end

      batch
      |> Batch.changeset(%{state: status, commit: commit, last_polled: now})
      |> Repo.update!()

      send_zulip(batch, status)

      # Now that the batch state is committed, reflect it in the queue labels:
      # a `:running` batch is "building", while a failed merge (:conflict /
      # :error) takes its patches off the queue.
      Labeler.reconcile_queue(repo_conn, batch.into_branch, Enum.map(patch_links, & &1.patch))

      Project.ping!(batch.project_id)
      status
    end
  end

  defp local_import_preflight(batch, links, candidate, result) do
    if Train.tracked?(candidate) do
      :dispatch
    else
      if Process.get(:orchestration_executor, false),
        do: ensure_no_withdrawal!(batch, patch_ids(candidate))

      BorsNG.Worker.FailureIsolation.preflight(links, batch, result)
    end
  end

  defp assemble_candidate_batch(batch, links, base, candidate, "local") do
    conn = get_repo_conn(batch.project)

    with :ok <- BorsNG.Worker.LocalGit.capacity(links),
         {:ok, toml} <- Batcher.GetBorsToml.get_required(conn, base.commit),
         true <- toml.use_squash_merge and batch.project.staging_branch == "staging",
         {:ok, result, context} <- local_assembly(batch, links, candidate, toml),
         expected <-
           if(Train.tracked?(candidate),
             do: String.duplicate("0", 40),
             else: GitHub.get_branch!(conn, batch.project.staging_branch).commit
           ),
         :ok <- BorsNG.Worker.CandidateAssembler.validate_result(candidate, result),
         :ok <-
           phase(batch, :assembly_live_heads, fn ->
             local_live_heads(conn, links, batch.project)
           end),
         :dispatch <- local_import_preflight(batch, links, candidate, result) do
      if Train.tracked?(candidate) do
        CandidateJournal.event(candidate, "train_barrier_checked", "bors.local_git", %{
          commit: result.commit,
          tree: result.tree,
          base_sha: result.base_sha,
          evidence: result.evidence["barriers"]
        })
      end

      setup_statuses(batch, toml)

      if Process.get(:orchestration_executor, false) do
        context =
          if Train.tracked?(candidate),
            do: Map.put(context, :staging_ref, Train.ref(candidate)),
            else: context

        publisher = %{kind: :local_git, result: result, context: context, expected: expected}

        dispatch_staging(
          batch,
          conn,
          result.commit,
          base.commit,
          result.tree,
          base.commit,
          toml,
          "bors.local_git",
          publisher
        )

        {:running, result.commit}
      else
        case BorsNG.Worker.LocalGit.push(result, context, expected) do
          %{"status" => "ok"} ->
            CandidateJournal.event(candidate, "staging_pushed", "bors.local_git", %{
              commit: result.commit,
              expected: expected,
              ref: "refs/heads/staging"
            })

            dispatch_staging(
              batch,
              conn,
              result.commit,
              base.commit,
              result.tree,
              base.commit,
              toml,
              "bors.local_git"
            )

            {:running, result.commit}

          %{"details" => details} ->
            CandidateJournal.event(candidate, "local_push_error", "bors.local_git", details)
            {:waiting, nil}
        end
      end
    else
      :partitioned ->
        poll_after_delay(batch.project)
        {:error, nil}

      false ->
        if Train.tracked?(candidate),
          do: {:waiting, nil},
          else: assemble_candidate_batch(batch, links, base, candidate, "off")

      {:conflict, details} ->
        CandidateJournal.event(candidate, "local_conflict", "bors.local_git", details)

        CandidateJournal.event(candidate, "failure_classified", "bors.local_git", %{
          classification: "merge_conflict",
          evidence: Map.drop(details, ["hints"]),
          proof_authority: false
        })

        start_waiting_merged_batch(batch, links, base, :conflict)

      :race ->
        start_waiting_merged_batch(batch, links, base, :race)

      {:error, %{"kind" => "member_limit"} = details} ->
        # Reuse durable, bundle-aware retry creation; never retry an unchanged
        # permanently unsupported formation or split an atomic bundle.
        state = Divider.split_batch(links, batch)

        CandidateJournal.event(candidate, "local_capacity_partitioned", "bors.local_git", %{
          error: details,
          members: length(links),
          result: state
        })

        if state == :failed do
          patches = Enum.map(links, & &1.patch)
          QueueApproval.fail(patches, batch, :member_limit)
          Bundles.drop_held_approvals(patches)
          Labeler.mark_awaiting_requeue(conn, batch.into_branch, patches)
        end

        poll_after_delay(batch.project)
        {:error, nil}

      {:error, details} ->
        CandidateJournal.event(candidate, "local_error", "bors.local_git", %{error: details})
        # Unavailable evidence retains immutable heads and standing approvals.
        {:waiting, nil}

      _ ->
        {:waiting, nil}
    end
  end

  defp assemble_candidate_batch(batch, patch_links, base, candidate, mode) do
    repo_conn = get_repo_conn(batch.project)
    stmp = "#{batch.project.staging_branch}.tmp"

    shadow =
      if mode == "shadow" do
        with {:ok, toml} <- Batcher.GetBorsToml.get_required(repo_conn, base.commit),
             true <- toml.use_squash_merge do
          local_assembly(batch, patch_links, candidate, toml)
        else
          _ -> nil
        end
      end

    Process.put({:local_shadow, batch.id}, shadow)

    try do
      tbase = %{
        tree: base.tree,
        commit:
          GitHub.synthesize_commit!(
            repo_conn,
            %{
              branch: stmp,
              tree: base.tree,
              parents: [base.commit],
              commit_message: "[ci skip][skip ci][skip netlify]",
              committer: nil
            }
          )
      }

      do_merge_patch = fn %{patch: patch, head_sha: approved_head}, branch ->
        pr = GitHub.get_pr!(repo_conn, patch.pr_xref)

        case branch do
          :conflict ->
            :conflict

          :canceled ->
            :canceled

          :race ->
            :race

          # Integrity invariant: never merge a head that differs from the
          # patch.commit everything upstream was authorized against. This live
          # head-vs-stored check is also the backstop the delegation merge-time
          # gate relies on: that gate verifies the *stored* patch.commit and
          # does not re-fetch, so if a `synchronize` was never processed it can
          # bless a stale-but-safe commit while the real head has moved on. This
          # comparison is what refuses that head. Weakening it (e.g. merging
          # without this live get_pr! check) would turn the gate's reliance on a
          # possibly-stale patch.commit into a real bypass. See
          # DELEGATION_INVALIDATION.md, "Known limitations — Missed-push window".
          _
          when pr.head_sha != patch.commit or
                 (approved_head != nil and approved_head != patch.commit) ->
            :race

          _ ->
            GitHub.merge_branch!(
              repo_conn,
              %{
                from: patch.commit,
                to: stmp,
                commit_message:
                  "[ci skip][skip ci][skip netlify] -bors-staging-tmp-#{patch.pr_xref}"
              }
            )
        end
      end

      merge = Enum.reduce(patch_links, tbase, do_merge_patch)

      start_waiting_merged_batch(batch, patch_links, base, merge)
    after
      GitHub.delete_branch!(repo_conn, stmp)
      Process.delete({:local_shadow, batch.id})
      Process.delete({:squash_tree, batch.id})
    end
  end

  defp phase(batch, name, fun) do
    if job = Process.get(:orchestration_job) do
      started = System.monotonic_time(:millisecond)

      try do
        fun.()
      after
        service_ms = System.monotonic_time(:millisecond) - started

        :telemetry.execute([:bors, :outbox, :phase], %{service_ms: service_ms}, %{
          job_id: job.id,
          project_id: batch.project_id,
          batch_id: batch.id,
          phase: name
        })

        Logger.info(
          "outbox_phase job=#{job.id} batch=#{batch.id} phase=#{name} service_ms=#{service_ms}"
        )
      end
    else
      fun.()
    end
  end

  defp local_assembly(batch, links, candidate, toml) do
    conn = get_repo_conn(batch.project)

    metadata =
      phase(batch, :preparation_metadata, fn ->
        deadline = System.monotonic_time(:millisecond) + 8_000

        prefetched =
          if GitHub.BatchRead.enabled?(batch.project) do
            GitHub.BatchRead.fetch(
              conn,
              batch.project.name,
              Enum.map(links, & &1.patch.pr_xref),
              :metadata,
              deadline
            )
          else
            {:ok, %{}}
          end

        with {:ok, prefetched} <- prefetched,
             {:ok, prefetched} <- resolve_prefetched_authors(conn, prefetched, deadline) do
          Enum.reduce_while(links, {:ok, []}, fn link, {:ok, members} ->
            if link.head_sha != link.patch.commit do
              {:halt, :race}
            else
              case prefetched_squash_data(
                     conn,
                     link.patch,
                     toml,
                     Map.get(prefetched, link.patch.pr_xref)
                   ) do
                {:ok, data} -> {:cont, {:ok, members ++ [Map.put(data, :pr, link.patch.pr_xref)]}}
                {:error, :head_changed} -> {:halt, :race}
                error -> {:halt, error}
              end
            end
          end)
        end
      end)

    with {:ok, members} <- metadata do
      context = %{
        repository: batch.project.name,
        conn: conn,
        members: members,
        date: DateTime.to_iso8601(DateTime.utc_now())
      }

      case phase(batch, :local_git_assembly, fn ->
             BorsNG.Worker.LocalGit.assemble(candidate, context)
           end) do
        {:ok, result} ->
          CandidateJournal.event(candidate, "local_assembled", "bors.local_git", result)
          {:ok, result, context}

        other ->
          other
      end
    end
  end

  defp local_live_heads(conn, links, project) do
    read =
      if GitHub.BatchRead.enabled?(project) do
        live =
          case GitHub.BatchRead.fetch(
                 conn,
                 project.name,
                 Enum.map(links, & &1.patch.pr_xref),
                 :identity
               ) do
            {:ok, live} -> live
            _ -> %{}
          end

        fn number -> Map.fetch(live, number) end
      else
        fn number -> GitHub.get_pr(conn, number) end
      end

    Enum.reduce_while(links, :ok, fn link, :ok ->
      case read.(link.patch.pr_xref) do
        {:ok, pr} when pr.head_sha == link.head_sha -> {:cont, :ok}
        {:ok, _} -> {:halt, :race}
        _ -> {:halt, {:error, :live_head_unavailable}}
      end
    end)
  end

  # A read-only shadow of an already running immutable generation avoids
  # waiting for another batch just to establish rollout parity. It allocates
  # no generation, pushes no ref and cannot change CI/status/promotion state.
  defp maybe_shadow_running(batch) do
    conn = get_repo_conn(batch.project)
    candidate = CandidateJournal.latest(batch.id)

    if not is_nil(candidate) and
         BorsNG.Worker.LocalGit.mode(batch.project, batch.into_branch, conn) == "shadow" and
         CandidateJournal.commit_binding(candidate)["commit"] == batch.commit and
         not Repo.exists?(
           from(e in BorsNG.Database.CandidateEvent,
             where:
               e.candidate_id == ^candidate.id and
                 e.kind in ["shadow_compared", "shadow_unavailable"]
           )
         ) do
      links = Repo.all(LinkPatchBatch.from_batch(batch.id)) |> Bundles.sort_links_for_merge()

      result =
        with {:ok, toml} <-
               Batcher.GetBorsToml.get_required(conn, candidate.snapshot["base_sha"]),
             true <- toml.use_squash_merge,
             {:ok, result, _context} <- local_assembly(batch, links, candidate, toml),
             :ok <- BorsNG.Worker.CandidateAssembler.validate_result(candidate, result),
             {:ok, tree} <- GitHub.get_commit_tree(conn, batch.commit) do
          CandidateJournal.event(candidate, "shadow_compared", "bors.local_git", %{
            matches: result.tree == tree,
            local_tree: result.tree,
            github_tree: tree,
            github_commit: batch.commit,
            local_commit: result.commit,
            replay: "running_immutable_candidate"
          })

          :ok
        end

      if result != :ok do
        CandidateJournal.event(candidate, "shadow_unavailable", "bors.local_git", %{
          result: inspect(result)
        })
      end
    end
  end

  defp compare_local_shadow(batch, tree) do
    case Process.get({:local_shadow, batch.id}) do
      {:ok, result, _context} ->
        CandidateJournal.event(
          Repo.get!(BorsNG.Database.Candidate, batch.candidate_id),
          "shadow_compared",
          "bors.local_git",
          %{
            matches: result.tree == tree,
            local_tree: result.tree,
            github_tree: tree,
            local_commit: result.commit
          }
        )

      nil ->
        :ok

      other ->
        CandidateJournal.event(
          Repo.get!(BorsNG.Database.Candidate, batch.candidate_id),
          "shadow_unavailable",
          "bors.local_git",
          %{result: inspect(other)}
        )
    end
  end

  # Private function to handle retry logic with exponential backoff
  defp get_base(repo_conn, into_branch) do
    do_get_base(repo_conn, into_branch, 1_000, 5_000)
  end

  defp do_get_base(repo_conn, into_branch, current_delay, max_delay) do
    # Non fast forward errors sometimes occur because GitHub returns a stale commit
    # when we fetch the target branch. Usually, this is because the last batch succeeded,
    # but GitHub has not propagated that info to all its APIs yet,
    # so the stale commit is the one the last successful batch was pushed on top of.
    # Therefore we cache the last commit we received from GitHub in the process dictionary below,
    # and then if the next time we call the GitHub branch API we receive the same commit,
    # we retry the API call with an exponentially increasing delay, until max_delay is exceeded.
    last_commit = Process.get(:last_commit)

    result =
      GitHub.get_branch!(
        repo_conn,
        into_branch
      )

    if Application.get_env(:bors, :is_test) do
      Process.put(:last_commit, result.commit)
      result
    else
      case result do
        %{commit: ^last_commit} when current_delay <= max_delay ->
          Process.sleep(current_delay)
          do_get_base(repo_conn, into_branch, current_delay * 2, max_delay)

        %{commit: ^last_commit} ->
          # Exceeded max delay but commit unchanged
          Logger.warning(
            "get_base: exceeded max delay but commit unchanged: #{inspect(last_commit)}"
          )

          result

        res ->
          # Commit changed or first call
          # Note that we reset the last_commit to :nil if the batch fails;
          # see the :error case of complete_batch/3
          Process.put(:last_commit, res.commit)

          Logger.info(
            "get_base: commit changed: #{inspect(last_commit)}. current_delay was #{current_delay}."
          )

          res
      end
    end
  end

  defp local_conflict(batch) do
    candidate = CandidateJournal.latest(batch.id)

    if candidate do
      event =
        Repo.one(
          from(e in BorsNG.Database.CandidateEvent,
            where:
              e.candidate_id == ^candidate.id and e.kind == "local_conflict" and
                e.source == "bors.local_git",
            order_by: [desc: e.id],
            limit: 1
          )
        )

      if event, do: event.details, else: %{}
    else
      %{}
    end
  end

  defp start_waiting_merged_batch(_batch, [], _, _) do
    {:canceled, nil}
  end

  defp start_waiting_merged_batch(batch, patch_links, base, %{tree: tree}) do
    repo_conn = get_repo_conn(batch.project)
    patches = Enum.map(patch_links, & &1.patch)

    policy_ref =
      if BorsNG.Worker.ManualApproval.scoped?(batch.project, batch.into_branch),
        do: base.commit,
        else: "#{batch.project.staging_branch}.tmp"

    repo_conn
    |> Batcher.GetBorsToml.get(policy_ref)
    |> case do
      {:ok, toml} ->
        parents =
          if toml.use_squash_merge do
            stmp = "#{batch.project.staging_branch}-squash-merge.tmp"
            GitHub.force_push!(repo_conn, base.commit, stmp)

            new_head =
              Enum.reduce_while(patch_links, base.commit, fn patch_link, prev_head ->
                Logger.debug("Patch Link #{inspect(patch_link)}")
                Logger.debug("Patch #{inspect(patch_link.patch)}")

                cpt =
                  case get_squash_pr_data(repo_conn, patch_link.patch, toml) do
                    {:ok,
                     %{
                       commit_message: commit_message,
                       source_sha: source_sha,
                       committer: committer
                     }} ->
                      Logger.info("Staging branch #{stmp}")
                      Logger.info("Commit sha #{source_sha}")

                      # Create a merge commit for each PR
                      # because each PR is merged on top of each other in stmp, we can verify against any merge conflicts
                      merge_commit =
                        GitHub.merge_branch!(
                          repo_conn,
                          %{
                            from: source_sha,
                            to: stmp,
                            commit_message:
                              "[ci skip][skip ci][skip netlify] -bors-staging-tmp-#{source_sha}"
                          }
                        )

                      Logger.info("Merge Commit #{inspect(merge_commit)}")

                      Logger.info("Previous Head #{inspect(prev_head)}")

                      # Then compress the merge commit into tree into a single commit
                      # append it to the previous commit
                      # Because the merges are iterative the contain *only* the changes from the PR vs the previous PR(or head)
                      case merge_commit do
                        :conflict ->
                          :conflict

                        _ ->
                          case GitHub.create_commit(
                                 repo_conn,
                                 %{
                                   tree: merge_commit.tree,
                                   parents: [prev_head],
                                   commit_message: commit_message,
                                   committer: committer
                                 }
                               ) do
                            {:ok, commit_sha} ->
                              Process.put({:squash_tree, batch.id}, merge_commit.tree)
                              commit_sha

                            {:error, :create_commit, status, _body, request_id} = error ->
                              Logger.warning(
                                "start_waiting_merged_batch: create_commit failed with status #{status}, request_id=#{inspect(request_id)}"
                              )

                              error

                            {:error, :create_commit} = error ->
                              Logger.warning("start_waiting_merged_batch: create_commit failed")
                              error
                          end
                      end

                    {:error, :head_changed} ->
                      :race

                    {:error, reason} = error ->
                      Logger.warning(
                        "start_waiting_merged_batch: unable to build squash commit metadata for PR #{patch_link.patch.pr_xref}: #{inspect(reason)}"
                      )

                      error
                  end

                Logger.info("Commit Sha #{inspect(cpt)}")

                case cpt do
                  :conflict ->
                    {:halt, :conflict}

                  :race ->
                    {:halt, :race}

                  {:error, _, _, _, _} = error ->
                    {:halt, error}

                  {:error, _} = error ->
                    {:halt, error}

                  commit_sha ->
                    {:cont, commit_sha}
                end
              end)

            GitHub.delete_branch!(repo_conn, stmp)

            case new_head do
              {:error, _, _, _, _} = error ->
                error

              {:error, _} = error ->
                error

              head ->
                [head]
            end
          else
            parents = [base.commit | Enum.map(patch_links, & &1.patch.commit)]
            parents
          end

        if toml.use_squash_merge do
          case parents do
            {:error, _, _, _, _} ->
              {:error, nil}

            {:error, _} ->
              {:error, nil}

            _ ->
              # This will avoid creating a merge commit, which is important since it will prevent
              # bors from polluting th git blame history with it's own name
              head = Enum.at(parents, 0)

              if head in [:conflict, :race] do
                start_waiting_merged_batch(batch, patch_links, base, head)
              else
                # Persist start intent before any write that can trigger CI.
                # A restart must not transfer this batch as unstarted work.
                tree = Process.get({:squash_tree, batch.id}, tree)
                compare_local_shadow(batch, tree)
                setup_statuses(batch, toml)

                unless Process.get(:orchestration_executor, false),
                  do: GitHub.force_push!(repo_conn, head, batch.project.staging_branch)

                dispatch_staging(batch, repo_conn, head, base.commit, tree, policy_ref, toml)
                {:running, head}
              end
          end
        else
          commit_message =
            Batcher.Message.generate_commit_message(
              patch_links,
              toml.cut_body_after,
              gather_co_authors(batch, patch_links),
              toml.commit_title
            )

          setup_statuses(batch, toml)

          head =
            create_staging_commit!(
              repo_conn,
              %{
                branch: batch.project.staging_branch,
                tree: tree,
                parents: parents,
                commit_message: commit_message,
                committer: toml.committer
              }
            )

          dispatch_staging(batch, repo_conn, head, base.commit, tree, policy_ref, toml)
          {:running, head}
        end

      {:error, message} ->
        message = Batcher.Message.generate_bors_toml_error(message)

        unless Process.get({:config_failure_reported, batch.id}) == message do
          send_message(repo_conn, patches, {:config, message})
          Process.put({:config_failure_reported, batch.id}, message)
        end

        Repo.update_all(from(b in Batch, where: b.id == ^batch.id),
          set: [recovery_after: DateTime.to_unix(DateTime.utc_now()) + 300]
        )

        {:waiting, nil}
    end
  end

  defp start_waiting_merged_batch(batch, patch_links, _base, :conflict) do
    project = batch.project
    repo_conn = get_repo_conn(project)
    patches = Enum.map(patch_links, & &1.patch)
    state = BorsNG.Worker.FailureIsolation.split(patch_links, batch, local_conflict(batch))
    poll_after_delay(project)

    if state == :failed do
      QueueApproval.fail(patches, batch, :conflict)
      # A single unmergeable PR is dropped. Flag it for a maintainer to
      # re-queue. The queue labels come off once the batch state is :conflict.
      Labeler.mark_awaiting_requeue(repo_conn, batch.into_branch, patches)

      # A terminal conflict cannot be bisected, and rebasing will not
      # resolve a member-versus-member clash. Drop the bundled patches'
      # held approvals and tell each bundle's members together, not per-PR.
      # Re-queueing needs a fresh r+ on every member, so a clean sibling's
      # r+ cannot silently re-queue the still-conflicting set. Keep the
      # retarget record intact for a later unlink to restore.
      {bundled, solo} = Enum.split_with(patches, &(&1.bundle_id != nil))
      send_message(repo_conn, solo, {:conflict, :failed, batch.into_branch})
      fail_bundles(repo_conn, bundled, &{:bundle_conflict, &1})
    else
      send_message(repo_conn, patches, {:conflict, state})
    end

    {:conflict, nil}
  end

  # At least one member raced: its live head is not its stored commit (a push
  # arrived whose synchronize webhook was never processed), or it was approved
  # at an earlier head than the stored one. Staging stops at the first raced
  # member and reports the whole batch, so every member is checked here.
  #
  # A raced member is reconciled against GitHub — its stale record refreshed
  # and its held approval dropped, the two effects the missed webhook would
  # have had — and fails with its bundle; nothing re-queues it without a fresh
  # r+. Without the refresh, a fresh r+ preflights the stale commit and races
  # again; with it, the next r+ (and the delegation merge-time gate) evaluates
  # the real head. The other members were approved at the heads they still
  # have, so they are re-queued in a new batch, as when a member is canceled
  # out of a running batch, and staging checks their heads again. A member
  # whose head cannot be read is deferred without revoking its approval.
  defp start_waiting_merged_batch(batch, patch_links, _base, :race) do
    project = batch.project
    repo_conn = get_repo_conn(project)
    classifications = Enum.map(patch_links, &{&1, raced_link?(repo_conn, project, &1)})

    if Enum.any?(classifications, fn {_, result} -> result == :unknown end) do
      # An API outage is not evidence of a changed head. Retain this batch and
      # let the regular poll retry; do not manufacture failed individual PRs.
      {:waiting, nil}
    else
      poll_after_delay(project)
      raced = for {link, true} <- classifications, do: link
      raced_ids = Enum.flat_map(raced, &Bundles.member_ids(patch_links, &1.patch_id))

      {raced_links, kept_links} =
        case raced_ids do
          [] -> {[], patch_links}
          _ -> Enum.split_with(patch_links, &(&1.patch_id in raced_ids))
        end

      raced_patches = Enum.map(raced_links, & &1.patch)
      QueueApproval.stop(raced_patches, :revision_changed)
      send_message(repo_conn, raced_patches, :race)

      if kept_links != [] do
        Divider.clone_batch(kept_links, project.id, batch.into_branch, "member_removed")

        send_message(
          repo_conn,
          Enum.map(kept_links, & &1.patch),
          if(raced == [],
            do: :race_unconfirmed,
            else: {:race, :retrying, Enum.map(raced, & &1.patch.pr_xref)}
          )
        )
      end

      send_status(repo_conn, batch, :race)

      {:error, nil}
    end
  end

  defp create_staging_commit!(conn, info) do
    if Process.get(:orchestration_executor, false),
      do: GitHub.create_commit!(conn, Map.delete(info, :branch)),
      else: GitHub.synthesize_commit!(conn, info)
  end

  defp dispatch_staging(
         batch,
         repo_conn,
         head,
         base,
         tree,
         policy_ref,
         toml,
         source \\ "github.commit_response",
         publisher \\ :github
       ) do
    candidate = Repo.get!(BorsNG.Database.Candidate, batch.candidate_id)

    toolchain = commit_toolchain(repo_conn, head, publisher)

    if Process.get(:orchestration_executor, false) do
      ensure_no_withdrawal!(batch, patch_ids(candidate))

      {:ok, _} =
        Repo.transaction(fn ->
          CandidateJournal.bind(candidate, head, tree, policy_ref, toml, toolchain, source)

          batch
          |> Batch.changeset(%{base_sha: base, commit: head, state: :running})
          |> Repo.update!()

          Outbox.enqueue(
            batch.project_id,
            "dispatch",
            {batch.id, publisher},
            "dispatch:#{batch.id}"
          )
        end)
    else
      dispatch_staging_serial(
        batch,
        repo_conn,
        head,
        base,
        tree,
        policy_ref,
        toml,
        toolchain,
        candidate,
        source
      )
    end
  end

  defp commit_toolchain(_conn, head, %{kind: :local_git, result: result}) do
    evidence =
      result.evidence["toolchain"] ||
        %{"content" => nil, "unknown_reason" => "local object read unavailable"}

    content = evidence["content"]

    %{
      ref: head,
      path: "lean-toolchain",
      content: content,
      sha256: if(is_binary(content), do: CandidateJournal.digest(content)),
      source: "bors.local_git_object",
      unknown_reason: evidence["unknown_reason"]
    }
  end

  defp commit_toolchain(repo_conn, head, _) do
    case GitHub.get_file(repo_conn, head, "lean-toolchain") do
      {:ok, content} when is_binary(content) ->
        %{
          ref: head,
          path: "lean-toolchain",
          content: content,
          sha256: CandidateJournal.digest(content),
          source: "github.contents"
        }

      {:ok, nil} ->
        %{
          ref: head,
          path: "lean-toolchain",
          content: nil,
          source: "github.contents",
          unknown_reason: "file absent"
        }

      _ ->
        %{
          ref: head,
          path: "lean-toolchain",
          content: nil,
          source: nil,
          unknown_reason: "read failed"
        }
    end
  end

  @doc "Replay only the current bound candidate; never reconstruct an accepted dispatch."
  def resume_dispatch(batch_id, publisher \\ :github) do
    case Repo.get(Batch, batch_id) do
      %Batch{state: :running} = batch ->
        batch = Repo.preload(batch, :project)
        candidate = CandidateJournal.current(batch)

        if candidate do
          binding = CandidateJournal.commit_binding(candidate)

          result =
            BorsNG.Worker.CandidateAssembler.identity(candidate)
            |> Map.merge(%{commit: batch.commit, tree: binding["tree"]})

          case BorsNG.Worker.CandidateAssembler.validate_result(candidate, result) do
            :ok ->
              resume_current_dispatch(batch, candidate, publisher)

            {:error, reason} ->
              CandidateJournal.event(candidate, "dispatch_rejected", "bors.outbox", %{
                reason: reason
              })

              # Reuse normal cancellation/retry semantics; stale members cannot
              # leave an active candidate owning the staging slot forever.
              links = Repo.all(LinkPatchBatch.from_batch(batch.id))

              Enum.each(links, fn link ->
                if link.patch.commit != link.head_sha,
                  do: do_handle_cast({:cancel, link.patch_id, :changed_head}, batch.project_id)
              end)

              case Repo.get(Batch, batch.id) do
                %Batch{state: :running} = current ->
                  Divider.clone_batch(
                    Repo.all(LinkPatchBatch.from_batch(batch.id)),
                    batch.project_id,
                    batch.into_branch,
                    "stale_dispatch"
                  )

                  current |> Batch.changeset(%{state: :canceled}) |> Repo.update!()

                _ ->
                  :ok
              end
          end
        end

      _ ->
        :ok
    end
  end

  defp resume_current_dispatch(batch, candidate, publisher) do
    import Ecto.Query

    events =
      Repo.all(from(e in BorsNG.Database.CandidateEvent, where: e.candidate_id == ^candidate.id))

    unless Enum.any?(events, &(&1.kind == "dispatched")) do
      case Train.guard(batch, get_repo_conn(batch.project)) do
        {:invalidate, reason} ->
          Train.invalidate(batch, reason)
          throw(:train_invalidated)

        {:hold, :withdrawal_pending} ->
          throw(:withdrawal_pending)

        _ ->
          :ok
      end

      if Train.tracked?(candidate) do
        if Train.reserve(candidate, get_repo_conn(batch.project)) != {:ok, :ok},
          do: throw(:staging_cap)

        if candidate.snapshot["speculative_predecessor_id"] &&
             not Train.barrier_free?(candidate, get_repo_conn(batch.project)) do
          Train.invalidate(batch, :policy_or_toolchain_barrier)
          throw(:train_invalidated)
        end
      end

      ensure_no_withdrawal!(batch, patch_ids(candidate))
      conn = get_repo_conn(batch.project)
      phase(batch, :dispatch_live_members, fn -> ensure_live_members!(batch, conn) end)
      requested = Enum.find(events, &(&1.kind == "dispatch_requested"))
      # A single bounded Actions page reconciles an ambiguous repository_dispatch.
      # No retry is allowed on an unavailable or truncated lookup.
      seen =
        if requested && System.get_env("BORS_STAGE_DISPATCH_PROJECT") == batch.project.name do
          {:ok, seen} =
            GitHub.staging_dispatch_seen(
              conn,
              batch.project.name,
              batch.commit,
              batch.base_sha,
              batch.id,
              batch.into_branch,
              requested.observed_at
            )

          seen
        else
          false
        end

      if requested && Train.tracked?(candidate) && not seen, do: throw(:dispatch_unconfirmed)

      if seen do
        CandidateJournal.event(candidate, "dispatched", "github.actions_recovery_read", %{
          commit: batch.commit,
          base_sha: batch.base_sha
        })
      else
        if Train.tracked?(candidate) do
          case GitHub.active_staging_count(conn) do
            {:ok, count} when count < 3 -> :ok
            _ -> throw(:external_staging_cap)
          end
        end

        # A push to the same SHA is idempotent and never fires another push build.
        ensure_no_withdrawal!(batch, patch_ids(candidate))

        phase(batch, :dispatch_publication, fn ->
          publish_bound_candidate!(batch, candidate, conn, publisher)
        end)

        if System.get_env("BORS_STAGE_DISPATCH_PROJECT") == batch.project.name do
          ensure_no_withdrawal!(batch, patch_ids(candidate))

          unless requested,
            do:
              CandidateJournal.event(candidate, "dispatch_requested", "bors.outbox", %{
                commit: batch.commit,
                base_sha: batch.base_sha
              })

          members =
            Enum.map(candidate.snapshot["members"], &%{pr: &1["pr"], head_sha: &1["head_sha"]})

          :ok =
            GitHub.dispatch_staging_once(
              conn,
              batch.project.name,
              batch.commit,
              batch.base_sha,
              batch.id,
              members,
              batch.into_branch
            )

          CandidateJournal.event(candidate, "dispatched", "github.workflow_dispatch_response", %{
            commit: batch.commit,
            base_sha: batch.base_sha
          })
        else
          CandidateJournal.event(candidate, "dispatched", "github.staging_push_response", %{
            commit: batch.commit,
            base_sha: batch.base_sha
          })
        end
      end

      # A successful staging mutation is the start of the CI timeout budget.
      binding = CandidateJournal.commit_binding(candidate)
      timeout = binding["policy"]["effective"]["timeout_sec"] || batch.project.batch_timeout_sec

      batch
      |> Batch.changeset(%{timeout_at: DateTime.to_unix(DateTime.utc_now()) + timeout})
      |> Repo.update!()
    end

    :ok
  end

  defp publish_bound_candidate!(batch, _candidate, conn, :github),
    do: GitHub.force_push!(conn, batch.commit, batch.project.staging_branch)

  defp publish_bound_candidate!(batch, candidate, conn, %{kind: :local_git} = publisher) do
    ref = Map.get(publisher.context, :staging_ref, batch.project.staging_branch)

    current =
      case GitHub.get_branch(conn, ref) do
        {:ok, branch} -> branch.commit
        {:error, :get_branch, 404} -> String.duplicate("0", 40)
        {:error, :get_branch, 404, _} -> String.duplicate("0", 40)
        other -> throw({:staging_read_unavailable, other})
      end

    cond do
      current == batch.commit ->
        :ok

      current != publisher.expected ->
        throw(:staging_ref_changed)

      true ->
        # Mirrors are expendable. Recreate with the original metadata and date;
        # reject any result that differs from the durably bound commit/tree.
        result = BorsNG.Worker.LocalGit.push(publisher.result, publisher.context, current)

        unless result["status"] == "ok" do
          {:ok, rebuilt} = BorsNG.Worker.LocalGit.assemble(candidate, publisher.context)
          :ok = BorsNG.Worker.CandidateAssembler.validate_result(candidate, rebuilt)

          if rebuilt.commit != publisher.result.commit or rebuilt.tree != publisher.result.tree,
            do: throw(:reconstruction_mismatch)

          %{"status" => "ok"} = BorsNG.Worker.LocalGit.push(rebuilt, publisher.context, current)
        end

        CandidateJournal.event(candidate, "staging_pushed", "bors.local_git", %{
          commit: batch.commit,
          expected: current,
          ref: "refs/heads/" <> ref
        })
    end
  end

  defp dispatch_staging_serial(
         batch,
         repo_conn,
         head,
         base,
         tree,
         policy_ref,
         toml,
         toolchain,
         candidate,
         source
       ) do
    CandidateJournal.bind(candidate, head, tree, policy_ref, toml, toolchain, source)
    # The public observation must bind an override to the actual staging base,
    # rather than trusting caller-supplied dispatch metadata.
    batch |> Batch.changeset(%{base_sha: base, commit: head, state: :running}) |> Repo.update!()

    if System.get_env("BORS_STAGE_DISPATCH_PROJECT") == batch.project.name do
      CandidateJournal.event(candidate, "dispatch_requested", "bors.workflow_dispatch", %{
        commit: head,
        base_sha: base
      })

      members =
        Repo.all(LinkPatchBatch.from_batch(batch.id))
        |> Enum.map(&%{pr: &1.patch.pr_xref, head_sha: &1.head_sha})

      :ok =
        GitHub.dispatch_staging(
          repo_conn,
          batch.project.name,
          head,
          base,
          batch.id,
          members,
          batch.into_branch
        )

      CandidateJournal.event(candidate, "dispatched", "github.workflow_dispatch_response", %{
        commit: head,
        base_sha: base
      })
    else
      CandidateJournal.event(candidate, "dispatched", "github.staging_push_response", %{
        commit: head,
        base_sha: base
      })
    end

    :ok
  end

  defp raced_link?(repo_conn, project, %{patch: patch, head_sha: approved_head}) do
    case GitHub.get_pr(repo_conn, patch.pr_xref) do
      {:ok, %{head_sha: head_sha} = pr} when head_sha != patch.commit ->
        # A race means webhook delivery failed at least once. Recurring
        # warnings here mean it is broken; do not let this path heal that
        # silently.
        Logger.warning(
          "race: patch #{patch.id} (PR ##{patch.pr_xref}) had stale commit " <>
            "#{inspect(patch.commit)}, live head is #{inspect(head_sha)}; re-syncing"
        )

        Bundles.drop_held_approval(patch.id)
        resync_raced_patch(project, patch, pr)
        true

      {:ok, _} ->
        approved_head != nil and approved_head != patch.commit

      error ->
        Logger.warning(
          "race: could not reconcile patch #{patch.id} (PR ##{patch.pr_xref}): #{inspect(error)}"
        )

        :unknown
    end
  end

  # sync_patch upserts the author, so it needs the PR's user. A PR payload
  # without one shouldn't happen (GitHub reports deleted authors as "ghost"),
  # but do not let the recovery path crash the batcher over it.
  defp resync_raced_patch(project, _patch, %{user: user} = pr) when not is_nil(user) do
    Syncer.sync_patch(project.id, pr)
  end

  defp resync_raced_patch(_project, patch, _pr) do
    Logger.warning("race: PR ##{patch.pr_xref} has no user; skipping re-sync")
  end

  defp resolve_prefetched_authors(conn, prefetched, deadline) do
    Enum.reduce_while(prefetched, {:ok, %{}, %{}}, fn
      {number, :rest}, {:ok, data, authors} ->
        {:cont, {:ok, Map.put(data, number, :rest), authors}}

      {number, %{pr: pr} = value}, {:ok, data, authors} ->
        key = {pr.user.id, pr.user.login}

        result =
          Map.get_lazy(authors, key, fn ->
            squash_author_profile(pr, GitHub.public_user_bounded(conn, pr.user.login, deadline))
          end)

        case result do
          {:ok, email, name} ->
            value = Map.merge(value, %{email: email, name: name})
            {:cont, {:ok, Map.put(data, number, value), Map.put(authors, key, result)}}

          _ ->
            {:halt, {:error, :author_unavailable}}
        end
    end)
    |> case do
      {:ok, data, _authors} -> {:ok, data}
      error -> error
    end
  end

  defp prefetched_squash_data(conn, patch, toml, value) when value in [nil, :rest],
    do: get_squash_pr_data(conn, patch, toml)

  defp prefetched_squash_data(_conn, patch, toml, %{
         pr: pr,
         commits: commits,
         name: name,
         email: email
       }) do
    if pr.head_sha == patch.commit do
      {:ok,
       %{
         commit_message:
           Batcher.Message.generate_squash_commit_message(
             pr,
             commits,
             email,
             name,
             toml.cut_body_after
           ),
         source_sha: patch.commit,
         committer: %{name: name, email: email}
       }}
    else
      {:error, :head_changed}
    end
  end

  defp get_squash_pr_data(repo_conn, patch, toml) do
    with {:ok, pr} <- GitHub.get_pr(repo_conn, patch.pr_xref),
         true <- pr.head_sha == patch.commit,
         {:ok, commits} <- GitHub.get_pr_commits(repo_conn, patch.pr_xref),
         {:ok, user_email, user_name} <- get_squash_author(repo_conn, pr),
         {:ok, current_pr} <- GitHub.get_pr(repo_conn, patch.pr_xref),
         true <- current_pr.head_sha == patch.commit do
      commit_message =
        Batcher.Message.generate_squash_commit_message(
          pr,
          commits,
          user_email,
          user_name,
          toml.cut_body_after
        )

      {:ok,
       %{
         commit_message: commit_message,
         source_sha: patch.commit,
         committer: %{name: user_name, email: user_email}
       }}
    else
      false -> {:error, :head_changed}
      error -> error
    end
  end

  defp get_squash_author({token, _repo_xref}, pr) when not is_nil(pr.user),
    do: squash_author_profile(pr, GitHub.get_user_by_login(token, pr.user.login))

  defp get_squash_author(_, _), do: {:error, :missing_pr_user}

  defp squash_author_profile(pr, result) do
    case result do
      {:ok, nil} ->
        {:ok, "#{pr.user.id}+#{pr.user.login}@users.noreply.github.com", pr.user.login}

      {:ok, user} ->
        user_email =
          if user.email != nil do
            user.email
          else
            "#{user.id}+#{user.login}@users.noreply.github.com"
          end

        user_name = user.name || user.login
        {:ok, user_email, user_name}

      {:error, _} = error ->
        error
    end
  end

  def gather_co_authors(batch, patch_links) do
    repo_conn = get_repo_conn(batch.project)

    patch_links
    |> Enum.map(& &1.patch.pr_xref)
    |> Enum.flat_map(&GitHub.get_pr_commits!(repo_conn, &1))
    |> Enum.map(&"#{&1.author_name} <#{&1.author_email}>")
    |> Enum.uniq()
  end

  # Write the batch's CI status rows and arm its timeout.
  #
  # A previous attempt may have left rows behind. This runs before the caller
  # commits the batch to `:running`, and several GitHub calls sit in between
  # (see start_waiting_batch/1: the staging.tmp delete, then a commit status
  # per patch). If the batcher dies in that window — a dyno restart, or
  # `delete_branch!` giving up — the batch stays `:waiting` with its statuses
  # already written. Re-inserting them would then violate
  # `statuses_identifier_batch_id_index` on *every* later poll, wedging the
  # batch until something deleted it. So clear the batch's rows first and
  # rewrite them, in one transaction: either the batch comes out armed, or
  # nothing changed. Rows from an abandoned attempt describe a staging commit
  # that no longer exists, so dropping them is the correct reading — the same
  # thing put_incomplete_on_hold/2 does when it re-queues a running batch.
  defp setup_statuses(batch, toml) do
    now = DateTime.to_unix(DateTime.utc_now(), :second)

    {:ok, _} =
      Repo.transaction(fn ->
        batch.id
        |> Status.all_for_batch()
        |> Repo.delete_all()

        toml.status
        |> Enum.map(
          &%Status{
            batch_id: batch.id,
            identifier: &1,
            url: nil,
            state: :running
          }
        )
        |> Enum.each(&Repo.insert!/1)

        batch
        |> Batch.changeset(%{timeout_at: now + toml.timeout_sec})
        |> Repo.update!()
      end)

    :ok
  end

  defp poll_running_batch(batch, from_recheck \\ false) do
    if Outbox.enabled?() and Outbox.pending?(batch.project_id, "dispatch:#{batch.id}") do
      :ok
    else
      poll_dispatched_batch(batch, from_recheck)
    end
  end

  defp poll_dispatched_batch(batch, from_recheck) do
    maybe_shadow_running(batch)
    repo_conn = get_repo_conn(batch.project)

    case GitHub.get_commit_status(repo_conn, batch.commit) do
      {:ok, statuses} ->
        statuses
        |> Enum.each(fn {identifier, state} ->
          batch.id
          |> Status.get_for_batch(identifier)
          |> Repo.update_all(set: [state: state, identifier: identifier])

          CandidateJournal.status(batch, identifier, state, "github.status_poll")
        end)

        maybe_complete_batch(batch)

      error when is_tuple(error) and tuple_size(error) >= 2 and elem(error, 0) == :error ->
        Logger.warning(
          "poll_running_batch: get_commit_status failed for batch #{batch.id} commit #{batch.commit}: #{inspect(error)}"
        )

        now = DateTime.to_unix(DateTime.utc_now(), :second)

        batch
        |> Batch.changeset(%{last_polled: now})
        |> Repo.update!()

        if expired_staging?(batch) and not green_staged?(batch) do
          maybe_complete_batch_due(batch, true)
        else
          if from_recheck, do: schedule_recheck(batch, now + 60)
        end
    end
  end

  defp maybe_complete_batch(batch) do
    if Process.get(:orchestration_executor, false) and
         Outbox.pending?(batch.project_id, "dispatch:#{batch.id}") do
      :ok
    else
      maybe_complete_dispatched_batch(batch)
    end
  end

  defp maybe_complete_dispatched_batch(batch) do
    now = DateTime.to_unix(DateTime.utc_now())

    if (batch.promotion_retry_at || 0) <= now and (batch.recovery_after || 0) <= now do
      maybe_complete_batch_due(batch)
    end
  end

  defp maybe_complete_batch_due(batch, status_read_failed \\ false) do
    case Train.guard(batch, get_repo_conn(batch.project)) do
      :ok ->
        maybe_complete_valid_batch(batch, status_read_failed)

      {:hold, _} ->
        batch
        |> Batch.changeset(%{last_polled: DateTime.to_unix(DateTime.utc_now())})
        |> Repo.update!()

      {:invalidate, reason} ->
        Train.invalidate(batch, reason)
    end
  end

  defp maybe_complete_valid_batch(batch, status_read_failed) do
    statuses = Repo.all(Status.all_for_batch(batch.id))
    initial_status = Batcher.State.summary_database_statuses(statuses)

    inconclusive_deadline =
      expired_staging?(batch) and
        ((status_read_failed and initial_status != :ok) or statuses == [] or
           initial_status in [:running, :waiting])

    now = DateTime.to_unix(DateTime.utc_now(), :second)
    repo_conn = get_repo_conn(batch.project)

    {next_status, report_terminal} =
      if initial_status != :running or inconclusive_deadline do
        # some repositories require an OK status to push
        if initial_status == :ok and not inconclusive_deadline,
          do: send_status(repo_conn, batch, :ok)

        # need to change status (could go from :ok to :error if non-ff push)
        outcome =
          if inconclusive_deadline,
            do: defer_inconclusive_build(batch, :deadline),
            else: complete_batch(initial_status, batch, statuses)

        result =
          if outcome == :ci_retry do
            # The old attempt is terminal but its members are still queued.
            # Report their live intent without a false failure notification.
            send_status(repo_conn, batch, :waiting)
            {:canceled, false}
          else
            if outcome != :running, do: send_status(repo_conn, batch, outcome)
            {outcome, outcome != :running}
          end

        Project.ping!(batch.project_id)
        result
      else
        {initial_status, false}
      end

    batch
    |> Batch.changeset(%{state: next_status, last_polled: now})
    |> Repo.update!()

    if next_status != :running do
      if next_status != :ok, do: Train.invalidate_descendants(batch)

      CandidateJournal.event(
        CandidateJournal.current(batch),
        "terminal_observed",
        "bors.status_summary",
        %{commit: batch.commit, state: next_status}
      )

      cancel_recheck(batch.id)
      if report_terminal, do: send_zulip(batch, next_status)
    end

    if next_status != :running do
      # A successful merge deliberately leaves the PR's labels untouched: the PR
      # closes, so `ready-to-merge` / `bors-staging` (and `delegated`) freeze as a
      # best-effort historical record of how it merged. Only a failure — which
      # leaves the PR open — reconciles its queue labels off (`awaiting-requeue`
      # for a terminal build failure is set separately in complete_batch(:error)).
      if next_status != :ok do
        patches = batch.id |> Patch.all_for_batch() |> Repo.all()
        Labeler.reconcile_queue(repo_conn, batch.into_branch, patches)
      end

      if Process.get(:orchestration_executor, false),
        do:
          Outbox.enqueue(
            batch.project_id,
            "info",
            {:completion_poll, batch.id},
            "complete:#{batch.id}"
          ),
        else: poll_(batch.project_id)
    end
  end

  @spec complete_batch(Status.state(), Batch.t(), [Status.t()]) :: Batch.state() | :ci_retry
  defp complete_batch(:ok, batch, statuses) do
    conn = get_repo_conn(batch.project)
    candidate = CandidateJournal.current(batch)

    if Train.tracked?(candidate) do
      case Train.run(candidate, conn) do
        {:ok, %{"status" => "completed", "conclusion" => "success"}} ->
          complete_verified_batch(batch, statuses, conn)

        _ ->
          :running
      end
    else
      complete_verified_batch(batch, statuses, conn)
    end
  end

  defp complete_batch(:error, batch, statuses) do
    project = batch.project

    if staged_failure?(batch, statuses) do
      case GitHub.staging_failure(
             get_repo_conn(project),
             batch.commit,
             project.name,
             batch.id,
             batch.into_branch,
             batch.base_sha
           ) do
        {:ok, {:conclusive, evidence}} ->
          if Process.get(:orchestration_executor, false),
            do: BorsNG.Worker.FailureDiagnostics.observe(batch, evidence, get_repo_conn(project))

          BorsNG.Worker.FailureIsolation.classified(batch, "candidate_failure", evidence)
          complete_failed_batch(batch, statuses)

        {:ok, {:infrastructure, evidence}} ->
          BorsNG.Worker.FailureIsolation.classified(batch, "infrastructure", evidence)
          defer_inconclusive_build(batch, :infrastructure)

        {:ok, :conclusive} ->
          BorsNG.Worker.FailureIsolation.classified(batch, "candidate_failure")
          complete_failed_batch(batch, statuses)

        {:ok, :pending} ->
          defer_pending_result(batch)

        {:ok, :cancelled} ->
          defer_inconclusive_build(batch, :cancelled)

        _ ->
          defer_inconclusive_build(batch, :unavailable)
      end
    else
      complete_failed_batch(batch, statuses)
    end
  end

  defp complete_verified_batch(batch, statuses, conn) do
    if Process.get(:orchestration_executor, false) and promotion_already_visible?(batch, conn) do
      squash = cleanup_squash(CandidateJournal.current(batch))
      members = cleanup_members(batch, squash)

      {:ok, _} =
        Repo.transaction(fn ->
          CandidateJournal.event(
            CandidateJournal.current(batch),
            "promoted",
            "github.base_recovery_read",
            %{commit: batch.commit, base_sha: batch.base_sha, base: batch.into_branch}
          )

          batch |> Batch.changeset(%{state: :ok}) |> Repo.update!()
          QueueApproval.stop(Repo.all(Patch.all_for_batch(batch.id)), :merged)
          enqueue_cleanup(batch, members, squash, statuses)
        end)

      :ok
    else
      complete_green_batch(batch, statuses, conn)
    end
  end

  defp cleanup_members(_batch, false), do: []

  defp cleanup_members(batch, _squash) do
    Repo.all(LinkPatchBatch.from_batch(batch.id))
    |> Enum.map(fn link ->
      patch = link.patch

      %{
        patch_id: patch.id,
        pr: patch.pr_xref,
        head: link.head_sha || patch.commit,
        token: patch.orchestration_approval_token,
        bundle_id: patch.bundle_id
      }
    end)
  end

  defp merged_notice_body(batch, member, statuses) do
    body =
      Batcher.Message.generate_message(
        {:merged, :squashed, batch.into_branch, statuses},
        bundle_page_url([%Patch{bundle_id: member.bundle_id}])
      )

    (body || "Merged into #{batch.into_branch}.") <>
      "\n\nMerged revision: `#{member.head}`."
  rescue
    _ -> "Revision `#{member.head}` merged into `#{batch.into_branch}` as `#{batch.commit}`."
  end

  defp legacy_cleanup_squash(batch) do
    deadline = System.monotonic_time(:millisecond) + 8_000

    case GitHub.cleanup_policy_bounded(get_repo_conn(batch.project), batch.commit, deadline) do
      # Inferred legacy configurations never enable squash.
      {:ok, nil} ->
        false

      {:ok, content} ->
        case Batcher.BorsToml.new(content) do
          {:ok, toml} -> toml.use_squash_merge
          _ -> throw({:cleanup_terminal, :invalid_policy})
        end

      {:error, _, status} when status in 400..499 and status not in [401, 403, 429] ->
        throw({:cleanup_terminal, {:http_status, status}})

      _ ->
        throw(:cleanup_policy_unavailable)
    end
  end

  defp cleanup_squash(candidate) do
    if candidate,
      do:
        get_in(CandidateJournal.commit_binding(candidate), [
          "policy",
          "effective",
          "use_squash_merge"
        ])
  end

  defp enqueue_cleanup(batch, members, squash, statuses) do
    if BorsNG.Worker.Orchestration.Projections.enabled?() and squash != false and members != [] do
      candidate = CandidateJournal.current(batch)
      members = Enum.map(members, &Map.put(&1, :body, merged_notice_body(batch, &1, statuses)))

      Outbox.enqueue(
        batch.project_id,
        "info",
        {:post_merge_cleanup, batch.id, batch.commit, candidate && candidate.id, squash, members},
        "post-merge:#{batch.id}:#{batch.commit}"
      )
    end
  end

  defp cleanup_batch(project_id, id, commit, candidate_id) do
    case Repo.get(Batch, id) do
      %Batch{project_id: ^project_id, state: :ok, commit: ^commit} = batch ->
        candidate = CandidateJournal.current(batch)

        if (candidate && candidate.id) == candidate_id,
          do: Repo.preload(batch, :project)

      _ ->
        nil
    end
  end

  defp expand_cleanup(source, offset, project_id) do
    case Outbox.payload(source) do
      {:post_merge_cleanup, id, commit, candidate_id, _squash, members} ->
        if batch = cleanup_batch(project_id, id, commit, candidate_id) do
          conn = get_repo_conn(batch.project)
          # Four independent children per turn; source data is retained once.
          {:ok, _} =
            Repo.transaction(fn ->
              Repo.one!(from(p in Project, where: p.id == ^project_id, lock: "FOR UPDATE"))

              members
              |> Enum.slice(offset, 4)
              |> Enum.each(fn member ->
                Outbox.enqueue(
                  project_id,
                  "info",
                  {:close_merged_pr, id, commit, candidate_id, member},
                  "post-merge-member:#{source.id}:#{member.patch_id}"
                )

                # The historical merged-revision receipt survives later PR intent.
                key = "post-merge-notice:#{source.id}:#{member.patch_id}"
                marker = "<!-- bors-outbox #{key} -->"

                Outbox.enqueue(
                  project_id,
                  "notice",
                  {conn, member.pr, member.body <> "\n\n" <> marker, marker},
                  key
                )
              end)

              if offset + 4 < length(members) do
                Outbox.enqueue(
                  project_id,
                  "info",
                  {:expand_post_merge_cleanup, source.id, offset + 4},
                  "post-merge-expand:#{source.id}:#{offset + 4}"
                )
              end
            end)
        end

      _ ->
        :ok
    end

    {:noreply, project_id}
  end

  defp close_merged_pr(batch, member) do
    deadline = System.monotonic_time(:millisecond) + 8_000
    conn = get_repo_conn(batch.project)

    if cleanup_member_current?(batch, member) do
      ensure_no_withdrawal!(batch, [member.patch_id])

      case GitHub.get_pr_bounded(conn, member.pr, deadline) do
        {:ok, pr} ->
          cond do
            pr.head_sha != member.head or pr.base_ref != batch.into_branch or pr.draft ->
              # Reconsider a missed push/retarget through ordinary admission.
              Outbox.enqueue(batch.project_id, "cast", {:eligibility, member.pr})

            pr.state == :closed ->
              :ok

            cleanup_member_current?(batch, member) ->
              ensure_no_withdrawal!(batch, [member.patch_id])

              if cleanup_member_current?(batch, member) do
                prefix = "[Merged by Bors] - "

                title =
                  if String.starts_with?(pr.title, prefix), do: pr.title, else: prefix <> pr.title

                case GitHub.close_pr_bounded(conn, member.pr, title, deadline) do
                  :ok ->
                    :ok

                  {:error, _, status} when status in 400..499 and status not in [401, 403, 429] ->
                    throw({:cleanup_terminal, {:http_status, status}})

                  _ ->
                    throw(:post_merge_close_unavailable)
                end
              end

            true ->
              :ok
          end

        {:error, :get_pr, 404, _} ->
          :ok

        {:error, :get_pr, 404} ->
          :ok

        _ ->
          throw(:post_merge_read_unavailable)
      end
    end

    :ok
  end

  defp cleanup_member_current?(batch, member) do
    case Repo.get(Patch, member.patch_id) do
      %Patch{project_id: id, commit: head, orchestration_approval_token: token} ->
        id == batch.project_id and head == member.head and token == member.token

      _ ->
        false
    end
  end

  defp promotion_already_visible?(batch, conn) do
    main = GitHub.get_branch!(conn, batch.into_branch).commit

    cond do
      main == batch.commit ->
        true

      promotion_requested?(batch) ->
        case GitHub.compare_status(conn, batch.commit, main) do
          {:ok, status} -> status in [:ahead, :identical]
          _ -> throw(:promotion_recovery_unavailable)
        end

      true ->
        false
    end
  end

  defp promotion_requested?(batch) do
    case CandidateJournal.current(batch) do
      nil ->
        false

      candidate ->
        Repo.exists?(
          from(e in BorsNG.Database.CandidateEvent,
            where: e.candidate_id == ^candidate.id and e.kind == "promotion_requested"
          )
        )
    end
  end

  defp complete_green_batch(batch, statuses, conn) do
    links = Repo.all(LinkPatchBatch.from_batch(batch.id))

    if Process.get(:orchestration_executor, false),
      do: ensure_no_withdrawal!(batch, Enum.map(links, & &1.patch_id))

    # Approval must still exist for the exact diff when CI finishes, even if a
    # retarget, force push or r- arrived after the scope status was posted.
    manual = Enum.filter(links, &(&1.manual_approval != nil))

    result =
      Enum.reduce_while(manual, :ok, fn link, _ ->
        patch = link.patch

        verdict =
          if patch.manual_approval == link.manual_approval and patch.commit == link.head_sha,
            do: BorsNG.Worker.ManualApproval.preflight(conn, patch),
            else: {:error, :manual_approval_changed}

        case verdict do
          :ok -> {:cont, :ok}
          :waiting -> {:halt, :waiting}
          {:error, _} -> {:halt, {:changed, patch.id}}
        end
      end)

    case result do
      :waiting ->
        defer_promotion(batch, :approval_read)
        :running

      {:changed, patch_id} ->
        do_handle_cast({:cancel, patch_id, :eligibility_changed}, batch.project.id)
        :canceled

      :ok ->
        case draft_patches(batch) do
          [] -> push_completed_batch(batch, statuses)
          drafts -> abort_draft_merge(batch, drafts)
        end
    end
  end

  defp defer_pending_result(batch) do
    if Batch.timeout_is_past(batch) do
      # The CI deadline is still bounded; an incomplete run provides no
      # individual proof verdict. Retry through ordinary preflight instead.
      defer_inconclusive_build(batch, :deadline)
    else
      deadline = DateTime.to_unix(DateTime.utc_now()) + 60
      batch |> Batch.changeset(%{recovery_after: deadline}) |> Repo.update!()
      schedule_recheck(batch, deadline)

      :running
    end
  end

  defp defer_inconclusive_build(batch, cause) do
    if cause != :infrastructure,
      do: BorsNG.Worker.FailureIsolation.classified(batch, "inconclusive", %{cause: cause})

    count = (batch.recovery_failures || 0) + 1
    delay = min(300 * Integer.pow(2, min(count - 1, 4)), 3_600)
    # Never monopolize staging indefinitely on unverifiable CI evidence.
    # After three infrastructure observations, retry the ordinary preflight
    # while retaining intent, rather than fabricating a failed singleton.
    reset_attempt =
      cause in [:cancelled, :infrastructure, :deadline] or count >= 3 or
        Batch.timeout_is_past(batch)

    deadline = DateTime.to_unix(DateTime.utc_now()) + delay
    changes = %{recovery_failures: count, recovery_after: deadline}

    state =
      if reset_attempt do
        result =
          Repo.transaction(fn ->
            links = Repo.all(LinkPatchBatch.from_batch(batch.id))

            child =
              Divider.clone_batch(links, batch.project_id, batch.into_branch, "inconclusive_ci")

            if is_nil(child), do: Repo.rollback(:retry_membership_unavailable)

            if child.state == :waiting do
              child
              |> Batch.changeset(%{
                recovery_failures: max(child.recovery_failures || 0, count),
                recovery_after: max(child.recovery_after || 0, deadline)
              })
              |> Repo.update!()
            end

            # The staging commit has a unique index. Release it so a singleton
            # child can test the same SHA, keeping old links and CI URLs as history.
            batch
            |> Batch.changeset(Map.merge(changes, %{state: :canceled, commit: nil}))
            |> Repo.update!()
          end)

        case result do
          {:ok, _} ->
            Process.put(:last_commit, nil)
            :ci_retry

          {:error, _} ->
            batch |> Batch.changeset(changes) |> Repo.update!()
            schedule_recheck(batch, deadline)
            :running
        end
      else
        batch |> Batch.changeset(changes) |> Repo.update!()
        schedule_recheck(batch, deadline)
        :running
      end

    if count >= 3 and (count == 3 or rem(count, 6) == 0) do
      Zulip.send_message(
        "Bors CI recovery needs attention: #{batch.project.name} batch #{batch.id}; " <>
          "CI has no conclusive result. Approval and membership are retained; " <>
          "the infrastructure failure count is #{count} and retries back off up to hourly. " <>
          "Inspect CI and API access; this is not an individual proof-failure verdict."
      )
    end

    # Retain approval and membership. The normal waiting-batch preflight will
    # re-check heads and withdrawals before a cancelled build is retried.
    Logger.warning(
      "staging CI inconclusive batch=#{batch.id} cause=#{cause} retry=#{count} delay=#{delay}"
    )

    state
  end

  defp complete_failed_batch(batch, statuses) do
    project = batch.project
    repo_conn = get_repo_conn(project)
    erred = Enum.filter(statuses, &(&1.state == :error))

    patch_links =
      batch.id
      |> LinkPatchBatch.from_batch()
      |> Repo.all()

    patches = Enum.map(patch_links, & &1.patch)
    state = BorsNG.Worker.FailureIsolation.split(patch_links, batch, nil, repo_conn)

    # The batch failed, so it's OK to push the next batch on top of the same commit we saw
    # see do_get_base/4
    Process.put(:last_commit, nil)

    if state == :retrying do
      poll_after_delay(project)
      send_message(repo_conn, patches, {state, erred})
    else
      # Record confirmed failure. Dependency recovery decides between a retry
      # and an exclusive proof-repair handoff on the next eligibility heartbeat.
      QueueApproval.fail(patches, batch, :build)
      BorsNG.Worker.SourceCompatibility.proven_failure(batch, patch_links, repo_conn)
      # The queue labels are taken off once the batch state is committed to :error.
      Labeler.mark_awaiting_requeue(repo_conn, batch.into_branch, patches)

      # A bundle cannot be bisected to find a culprit. Drop the bundled
      # patches' held approvals (like a solo PR loses its r+) and tell each
      # bundle's members together, not per-PR text. Every member then needs
      # a fresh r+. Re-running requires re-reviewing the fixed pull request.
      # The bundle is not dissolved and bases stay retargeted, so the
      # retargeting record stays intact for a later unlink to restore.
      {bundled, solo} = Enum.split_with(patches, &(&1.bundle_id != nil))
      send_message(repo_conn, solo, {state, erred})
      fail_bundles(repo_conn, bundled, &{:bundle_failed, &1, erred})
    end

    :error
  end

  # The last gate before the push. A draft is not supposed to reach a batch —
  # `Command.run/1` refuses `r+` on one, and converting to draft cancels the
  # batch — but both hang on a webhook, and nothing between here and the merge
  # re-checks. `Divider`'s mergeability check is only reached once a batch has
  # already failed.
  defp draft_patches(batch) do
    repo_conn = get_repo_conn(batch.project)

    batch.id
    |> Patch.all_for_batch()
    |> Repo.all()
    |> Enum.sort_by(& &1.pr_xref)
    |> Enum.filter(&draft_at_merge?(repo_conn, &1))
  end

  # The synced column catches a draft event whose cleanup did not run; the live
  # read catches a delivery bors never received at all. A failed read falls
  # back to the column rather than holding up the merge over an API blip.
  #
  # The catch-all is load-bearing, not defensive padding. `GitHub.get_pr/2`
  # reports failure in three different shapes: `{:error, :get_pr, status,
  # pr_xref}` from the real server on any non-200, and
  # `{:error, :github_call_timeout, :get_pr}` /
  # `{:error, :github_call_exit, :get_pr, reason}` when the GitHub GenServer
  # is wedged. Only `ServerMock` returns a bare `{:error, :get_pr}`, so a
  # narrow `{:error, _}` clause type-checks against the tests and raises
  # `CaseClauseError` in production — inside the poll that is about to push,
  # which takes the batcher down and lets the registry bin the project's whole
  # queue. Match `reconcile_raced_patch/3` and swallow every shape.
  defp draft_at_merge?(repo_conn, patch) do
    case GitHub.get_pr(repo_conn, patch.pr_xref) do
      {:ok, pr} ->
        pr.draft

      error ->
        Logger.warning(
          "draft_at_merge?: get_pr failed for patch #{patch.id} (PR ##{patch.pr_xref}): " <>
            "#{inspect(error)}; falling back to synced is_draft=#{patch.is_draft}"
        )

        patch.is_draft
    end
  end

  # Drop the drafts and re-queue the rest, the way canceling one patch out of a
  # running batch does. Only the draft is at fault, and bors knows exactly
  # which one, so nothing else in the batch should have to be re-approved.
  defp abort_draft_merge(batch, drafts) do
    project = batch.project
    repo_conn = get_repo_conn(project)

    patch_links = batch.id |> LinkPatchBatch.from_batch() |> Repo.all()
    patches = Enum.map(patch_links, & &1.patch)
    draft_ids = MapSet.new(drafts, & &1.id)

    # A draft takes its bundle with it: linked patches merge together or not
    # at all.
    dropped_ids =
      drafts
      |> Enum.flat_map(&Bundles.member_ids(patch_links, &1.id))
      |> MapSet.new()

    {dropped, kept} = Enum.split_with(patches, &MapSet.member?(dropped_ids, &1.id))
    kept_links = Enum.reject(patch_links, &MapSet.member?(dropped_ids, &1.patch_id))

    # Nothing was pushed, so the next batch can build on the commit we saw.
    Process.put(:last_commit, nil)

    if kept_links != [] do
      Divider.clone_batch(kept_links, project.id, batch.into_branch, "member_removed")
      poll_after_delay(project)
      send_message(repo_conn, kept, {:canceled, :retrying})
    end

    # The drafts are dropped for good: re-queueing one would only stop here
    # again. They need a fresh r+ after being marked ready for review.
    Bundles.drop_held_approvals(dropped)
    QueueApproval.stop(dropped, :draft)
    Labeler.mark_awaiting_requeue(repo_conn, batch.into_branch, dropped)
    send_message(repo_conn, drafts, :draft_dropped_from_batch)

    siblings = Enum.reject(dropped, &MapSet.member?(draft_ids, &1.id))
    send_bundle_pulled(repo_conn, siblings, drafts, :draft)

    :canceled
  end

  defp patch_ids(candidate), do: Enum.map(candidate.snapshot["members"], & &1["patch_id"])

  defp live_member_reads(conn, project, links) do
    if GitHub.BatchRead.enabled?(project) do
      {:ok, live} =
        GitHub.BatchRead.fetch(conn, project.name, Enum.map(links, & &1.patch.pr_xref), :identity)

      fn number -> Map.fetch!(live, number) end
    else
      fn number -> GitHub.get_pr!(conn, number) end
    end
  end

  defp ensure_live_members!(batch, conn) do
    links = Repo.all(LinkPatchBatch.from_batch(batch.id))
    live = live_member_reads(conn, batch.project, links)

    Enum.each(links, fn link ->
      # Fail closed on an unavailable live read. Cached webhook heads cannot
      # authorize a prioritized dispatch while a push/close is still in transit.
      pr = live.(link.patch.pr_xref)

      if link.patch.commit != link.head_sha or pr.head_sha != link.head_sha or
           pr.state != :open or pr.draft or pr.base_ref != batch.into_branch do
        do_handle_cast({:cancel, link.patch_id, :changed_head}, batch.project_id)
        throw(:stale_candidate_member)
      end
    end)
  end

  defp ensure_no_withdrawal!(batch, ids) do
    job = Process.get(:orchestration_job)
    if job && not Outbox.owned?(job), do: throw(:stale_outbox_claim)
    if Outbox.withdrawal_pending?(batch.project_id, ids), do: throw(:withdrawal_pending)
  end

  defp push_completed_batch(batch, statuses) do
    project = batch.project
    repo_conn = get_repo_conn(project)

    candidate = CandidateJournal.current(batch)

    toml =
      if Train.tracked?(candidate) do
        # Promotion uses the policy recorded with this exact generation;
        # mutable staging refs may belong to another candidate by now.
        effective = CandidateJournal.commit_binding(candidate)["policy"]["effective"]
        %{use_squash_merge: effective["use_squash_merge"]}
      else
        {_, policy} =
          case Batcher.GetBorsToml.get(repo_conn, batch.project.staging_branch) do
            {:error, :fetch_failed} ->
              Batcher.GetBorsToml.get(repo_conn, "#{batch.project.staging_branch}.tmp")

            {:ok, x} ->
              {:ok, x}
          end

        policy
      end

    if Process.get(:orchestration_executor, false) do
      if candidate = CandidateJournal.current(batch) do
        binding = CandidateJournal.commit_binding(candidate)

        result =
          BorsNG.Worker.CandidateAssembler.identity(candidate)
          |> Map.merge(%{commit: batch.commit, tree: binding["tree"]})

        if BorsNG.Worker.CandidateAssembler.validate_result(candidate, result) != :ok,
          do: throw(:stale_promotion)
      end

      links = Repo.all(LinkPatchBatch.from_batch(batch.id))
      ensure_no_withdrawal!(batch, Enum.map(links, & &1.patch_id))

      ensure_live_members!(batch, repo_conn)
    end

    if Process.get(:orchestration_executor, false) and not promotion_requested?(batch) do
      CandidateJournal.event(
        CandidateJournal.current(batch),
        "promotion_requested",
        "bors.outbox",
        %{commit: batch.commit, base: batch.into_branch}
      )
    end

    case Train.guard(batch, repo_conn) do
      :ok ->
        :ok

      {:invalidate, reason} ->
        Train.invalidate(batch, reason)
        throw(:train_invalidated)

      {:hold, _} ->
        throw(:train_predecessor_pending)
    end

    members =
      if Process.get(:orchestration_executor, false) do
        links = Repo.all(LinkPatchBatch.from_batch(batch.id))
        ensure_no_withdrawal!(batch, Enum.map(links, & &1.patch_id))
        members = cleanup_members(batch, toml.use_squash_merge)
        members
      else
        []
      end

    push_result =
      if Train.tracked?(CandidateJournal.current(batch)) do
        BorsNG.Worker.LocalGit.promote(batch, repo_conn)
      else
        push_with_retry(repo_conn, batch.commit, batch.into_branch)
      end

    push_status =
      case push_result do
        {:error, :push, 422, raw_error_content} ->
          cond do
            String.contains?(raw_error_content, "Update is not a fast forward") ->
              {:non_ff}

            true ->
              {:unknown_failure, 422, raw_error_content}
          end

        {:error, _, status_code, raw_error_content} ->
          {:unknown_failure, status_code, raw_error_content}

        {:ok, _} ->
          {:success}
      end

    patches =
      batch.id
      |> Patch.all_for_batch()
      |> Repo.all()

    case push_status do
      {:success} ->
        {:ok, _} =
          Repo.transaction(fn ->
            CandidateJournal.event(
              CandidateJournal.current(batch),
              "promoted",
              "github.base_push_response",
              %{commit: batch.commit, base_sha: batch.base_sha, base: batch.into_branch}
            )

            if Process.get(:orchestration_executor, false) do
              batch |> Batch.changeset(%{state: :ok}) |> Repo.update!()
              QueueApproval.stop(patches, :merged)
              enqueue_cleanup(batch, members, toml.use_squash_merge, statuses)
            end
          end)

        if batch.into_branch == "main",
          do: BorsNG.Worker.SourceCompatibility.wake(batch.project, batch.commit)

        Process.delete({:push_failure_reported, batch.id})
        Process.delete({:promotion_retry, batch.id})

        unless Process.get(:orchestration_executor, false),
          do: QueueApproval.stop(patches, :merged)

        # The bundle (if any) has merged. Drop held approvals so a future
        # run needs fresh r+. Forget the retargeting record so a later unlink
        # does not try to restore a base whose changes are now merged.
        Bundles.drop_held_approvals(patches)
        Bundles.forget_retargeting(patches)

        if toml.use_squash_merge && BorsNG.Worker.Orchestration.Projections.enabled?() do
          :ok
        else
          if toml.use_squash_merge do
            Enum.each(patches, fn patch ->
              send_message(repo_conn, [patch], {:merged, :squashed, batch.into_branch, statuses})
              pr = GitHub.get_pr!(repo_conn, patch.pr_xref)
              pr = %BorsNG.GitHub.Pr{pr | state: :closed, title: "[Merged by Bors] - #{pr.title}"}
              GitHub.update_pr!(repo_conn, pr)
            end)
          else
            send_message(repo_conn, patches, {:succeeded, statuses})
          end
        end

        :ok

      {:non_ff} ->
        # retry the complete batch (no bisect required as build passed all statuses)
        patch_links =
          batch.id
          |> LinkPatchBatch.from_batch()
          |> Repo.all()

        Divider.clone_batch(patch_links, project.id, batch.into_branch, "non_fast_forward")
        poll_after_delay(project)

        # send appropriate message to failed patches
        send_message(repo_conn, patches, {:push_failed_non_ff, batch.into_branch})

        :error

      {:unknown_failure, status_code, raw_error_content} ->
        # Retain the green staging commit and its memberships. An inability to
        # promote it is an infrastructure failure, not a failed proof.

        # send appropriate message to failed patches
        unless Process.get({:push_failure_reported, batch.id}) == status_code do
          send_message(
            repo_conn,
            patches,
            {:push_failed_unknown_failure, batch.into_branch, status_code, raw_error_content}
          )

          Process.put({:push_failure_reported, batch.id}, status_code)
        end

        # Keep the existing tested commit and retry with a bounded cadence.
        defer_promotion(batch, status_code)

        :running
    end
  end

  defp defer_promotion(batch, reason) do
    count = (batch.promotion_failures || 0) + 1
    delay = if count < 6, do: 300, else: 3600

    Repo.update_all(from(b in Batch, where: b.id == ^batch.id),
      set: [
        promotion_retry_at: DateTime.to_unix(DateTime.utc_now()) + delay,
        promotion_failures: count
      ]
    )

    if count in [1, 6] do
      Logger.warning(
        "bors promotion deferred batch=#{batch.id} attempts=#{count} reason=#{inspect(reason)}"
      )

      if count == 6,
        do:
          Zulip.send_message(
            "Bors promotion stuck: #{batch.project.name} batch #{batch.id} has a green staging commit " <>
              "but promotion is unavailable after six attempts. Approvals are retained; retrying hourly. " <>
              "Check permissions/backend availability. A maintainer can withdraw a member with bors r-."
          )
    end

    schedule_recheck(
      batch,
      max(DateTime.to_unix(DateTime.utc_now()) + delay, batch.recovery_after || 0)
    )
  end

  # Terminal-failure handling for the bundled patches of a batch: drop every
  # member's held approval, so a sibling's stale approval cannot re-queue a
  # failed set on one fresh r+, and message each bundle's members together.
  # Grouped by bundle so this stays correct even if a terminally-failed batch
  # ever holds more than one bundle (today the divider returns a terminal
  # state only for a single unit).
  defp fail_bundles(_repo_conn, [], _message_fun), do: :ok

  defp fail_bundles(repo_conn, bundled, message_fun) do
    Bundles.drop_held_approvals(bundled)

    bundled
    |> Enum.group_by(& &1.bundle_id)
    |> Enum.each(fn {_bundle_id, members} ->
      xrefs = members |> Enum.map(& &1.pr_xref) |> Enum.sort()
      send_message(repo_conn, members, message_fun.(xrefs))
    end)
  end

  # A delay has been observed between Bors sending the Status change
  # and GitHub allowing a Status-bearing commit to be pushed to master.
  # As a workaround, retry with exponential backoff.
  # This should retry *nine times*, by the way.
  defp push_with_retry(repo_conn, commit, into_branch, timeout \\ 40) do
    result =
      GitHub.push(
        repo_conn,
        commit,
        into_branch
      )

    if Application.get_env(:bors, :is_test) do
      result
    else
      case result do
        {:ok, _} ->
          Logger.info("push_with_retry: succeeded when timeout was #{timeout}")
          result

        _ when timeout >= 20_480 ->
          Logger.warning("push_with_retry: failed when timeout was #{timeout}")
          result

        _ ->
          Process.sleep(timeout)
          push_with_retry(repo_conn, commit, into_branch, timeout * 2)
      end
    end
  end

  defp timeout_batch(batch) do
    project = batch.project
    repo_conn = get_repo_conn(project)

    patch_links =
      batch.id
      |> LinkPatchBatch.from_batch()
      |> Repo.all()

    patches = Enum.map(patch_links, & &1.patch)
    state = Divider.split_batch(patch_links, batch)

    if state == :retrying do
      poll_after_delay(project)
      send_message(repo_conn, patches, {:timeout, state})
    else
      # Terminal failure: the PRs are dropped and need a maintainer to put
      QueueApproval.fail(patches, batch, :timeout)
      # them back on the queue, so flag them before the queue reconcile below
      # takes `ready-to-merge` / `bors-staging` off.
      Labeler.mark_awaiting_requeue(repo_conn, batch.into_branch, patches)

      # A timed-out bundle fails as one unit, same as a build failure or
      # conflict: drop the held approvals so a sibling's stale approval
      # cannot re-queue the set on one fresh r+.
      {bundled, solo} = Enum.split_with(patches, &(&1.bundle_id != nil))
      send_message(repo_conn, solo, {:timeout, state})
      fail_bundles(repo_conn, bundled, &{:bundle_timeout, &1})
    end

    batch
    |> Batch.changeset(%{state: :error})
    |> Repo.update!()

    send_zulip(batch, :error)

    Project.ping!(project.id)

    send_status(repo_conn, batch, :timeout)

    # The batch is no longer :running, so its patches are off the queue. A
    # :retrying bisect re-queued its halves into fresh :waiting batches, which
    # this reconcile sees and keeps labeled. Mirrors maybe_complete_batch.
    Labeler.reconcile_queue(repo_conn, batch.into_branch, patches)
  end

  defp cancel_patch(nil, _, _), do: :ok

  defp cancel_patch(batch, patch_id, reason) do
    cancel_patch(batch, patch_id, batch.state, reason)
    Project.ping!(batch.project_id)
  end

  defp cancel_patch(batch, patch_id, :running, reason) do
    project = batch.project

    patch_links =
      batch.id
      |> LinkPatchBatch.from_batch()
      |> Repo.all()

    patches = Enum.map(patch_links, & &1.patch)

    # Canceling a bundled patch cancels its whole bundle. Linked patches
    # merge together or not at all.
    canceled_ids = Bundles.member_ids(patch_links, patch_id)

    uncanceled_patch_links =
      Enum.reject(patch_links, &(&1.patch_id in canceled_ids))

    state =
      case uncanceled_patch_links do
        [] -> :failed
        _ -> :retrying
      end

    batch
    |> Batch.changeset(%{state: :canceled})
    |> Repo.update!()

    Train.invalidate_descendants(batch)
    send_zulip(batch, :canceled)

    repo_conn = get_repo_conn(project)

    if state == :retrying do
      Divider.clone_batch(uncanceled_patch_links, project.id, batch.into_branch, "cancellation")

      uncanceled_patches =
        Enum.reject(
          patches,
          &(&1.id in canceled_ids)
        )

      send_message(repo_conn, uncanceled_patches, {:canceled, :retrying})
    end

    canceled_patch = Enum.find(patches, &(&1.id == patch_id))
    siblings = Enum.filter(patches, &(&1.id in canceled_ids and &1.id != patch_id))

    send_message(repo_conn, [canceled_patch], {:canceled, :failed, reason})
    send_message(repo_conn, siblings, {:bundle_pulled, canceled_patch.pr_xref, reason})

    send_status(repo_conn, batch, :canceled)

    # The canceled patch (and its bundle) leave the queue. Any uncanceled
    # patches were re-queued into a fresh batch above, so reconcile reflects both.
    Labeler.reconcile_queue(repo_conn, batch.into_branch, patches)
  end

  defp cancel_patch(batch, patch_id, _state, reason) do
    project = batch.project

    patch_links =
      batch.id
      |> LinkPatchBatch.from_batch()
      |> Repo.all()

    # Canceling a bundled patch pulls its whole bundle from the waiting batch.
    # All members leave together.
    canceled_ids = Bundles.member_ids(patch_links, patch_id)

    canceled_links = Enum.filter(patch_links, &(&1.patch_id in canceled_ids))
    Enum.each(canceled_links, &Repo.delete!/1)

    if Batch.is_empty(batch.id, Repo) do
      Repo.delete!(batch)
    end

    canceled_patches = Enum.map(canceled_links, & &1.patch)
    patch = Repo.get!(Patch, patch_id)
    siblings = Enum.reject(canceled_patches, &(&1.id == patch_id))
    repo_conn = get_repo_conn(project)
    send_status(repo_conn, batch.id, canceled_patches, :canceled)
    send_message(repo_conn, [patch], {:canceled, :failed, reason})
    send_message(repo_conn, siblings, {:bundle_pulled, patch.pr_xref, reason})

    # The patches have been removed from their waiting batch, so they are off
    # the queue.
    Labeler.reconcile_queue(repo_conn, batch.into_branch, canceled_patches)
  end

  defp patch_preflight(repo_conn, patch) do
    result =
      if patch.manual_approval,
        do: BorsNG.Worker.ManualApproval.preflight(repo_conn, patch),
        else: BorsNG.Worker.MergeEligibility.preflight(repo_conn, patch)

    case result do
      :ok -> patch_preflight_policy(repo_conn, patch)
      other -> other
    end
  end

  defp patch_preflight_policy(repo_conn, patch) do
    if Patch.ci_skip?(patch) do
      {:error, :ci_skip}
    else
      toml =
        if BorsNG.Worker.ManualApproval.scoped?(
             Repo.get!(Project, patch.project_id),
             patch.into_branch
           ),
           do: Batcher.GetBorsToml.get_required(repo_conn, patch.into_branch),
           else: Batcher.GetBorsToml.get(repo_conn, patch.commit)

      patch_preflight(repo_conn, patch, toml)
    end
  end

  defp patch_preflight(_repo_conn, patch, {:error, _}) do
    if BorsNG.Worker.ManualApproval.scoped?(
         Repo.get!(Project, patch.project_id),
         patch.into_branch
       ),
       do: :waiting,
       else: {:ok, nil}
  end

  defp patch_preflight(repo_conn, patch, {:ok, toml}) do
    Delegation.reconcile_default_expiry(patch, toml.delegation_default_expiry_sec)

    with {:ok, labels} <- GitHub.get_labels(repo_conn, patch.pr_xref),
         :ok <- Batcher.Diversity.remember(patch, labels),
         {:ok, github_commit_statuses} <- GitHub.get_commit_status(repo_conn, patch.commit),
         {:ok, reviews} <- GitHub.get_reviews(repo_conn, patch.pr_xref),
         {:ok, commit_reviews} <- maybe_get_commit_reviews(repo_conn, patch, toml) do
      passed_label =
        labels
        |> MapSet.new()
        |> MapSet.disjoint?(MapSet.new(toml.block_labels))

      pr_status_mapset = MapSet.new(toml.pr_status)

      no_error_status =
        github_commit_statuses
        |> Enum.filter(fn {_, status} -> status == :error end)
        |> Enum.map(fn {context, _} -> context end)
        |> MapSet.new()
        |> MapSet.disjoint?(pr_status_mapset)

      no_waiting_status =
        github_commit_statuses
        |> Enum.filter(fn {_, status} -> status == :running end)
        |> Enum.map(fn {context, _} -> context end)
        |> MapSet.new()
        |> MapSet.disjoint?(pr_status_mapset)

      # We wait to have all required pr statuses set.
      no_unset_status =
        github_commit_statuses
        |> Enum.filter(fn {context, _} -> MapSet.member?(pr_status_mapset, context) end)
        |> Enum.count() == Enum.count(pr_status_mapset)

      code_owners_approved = check_code_owner(repo_conn, patch, toml)

      # fetching all reviews even when up_to_date_approvals is on to catch cases of rejected reviews.
      passed_review = reviews |> reviews_status(toml)

      passed_up_to_date_review =
        if toml.required_approvals && toml.up_to_date_approvals do
          commit_reviews |> reviews_status(toml)
        else
          :sufficient
        end

      Logger.info(
        "Code review status: Label Check #{passed_label} Passed Status: #{no_error_status and no_waiting_status and no_unset_status} Passed Review: #{passed_review} CODEOWNERS: #{code_owners_approved} Passed Up-To-Date Review: #{passed_up_to_date_review}"
      )

      case {passed_label, no_error_status, no_waiting_status, no_unset_status, passed_review,
            code_owners_approved, passed_up_to_date_review} do
        {true, true, true, true, :sufficient, true, :sufficient} ->
          {:ok, toml.max_batch_size}

        {false, _, _, _, _, _, _} ->
          {:error, :blocked_labels}

        {_, _, _, _, :insufficient, _, _} ->
          {:error, :insufficient_approvals}

        {_, _, _, _, :failed, _, _} ->
          {:error, :blocked_review}

        {_, _, _, _, _, false, _} ->
          {:error, :missing_code_owner_approval}

        {_, false, _, _, _, _, _} ->
          {:error, :pr_status}

        {_, _, false, _, _, _, _} ->
          {:waiting, toml}

        {_, _, _, false, _, _, _} ->
          {:waiting, toml}

        {_, _, _, _, _, _, :insufficient} ->
          {:error, :insufficient_up_to_date_approvals}
      end
    else
      error when is_tuple(error) and tuple_size(error) >= 2 and elem(error, 0) == :error ->
        Logger.warning(
          "patch_preflight: transient GitHub read failure for patch #{patch.id}: #{inspect(error)}"
        )

        {:waiting, toml}
    end
  end

  defp maybe_get_commit_reviews(repo_conn, patch, toml) do
    if toml.required_approvals && toml.up_to_date_approvals do
      GitHub.get_commit_reviews(repo_conn, patch.pr_xref, patch.commit)
    else
      {:ok, nil}
    end
  end

  # A poll armed for a member with a held approval reads the approval back
  # from the row when it fires. An r- during the wait ends the loop.
  defp approval_generation(patch) do
    if Process.get(:orchestration_executor, false) do
      token = Base.encode16(:crypto.strong_rand_bytes(16))
      query = from(p in Patch, where: p.id == ^patch.id)

      query =
        if is_nil(patch.orchestration_approval_token),
          do: from(p in query, where: is_nil(p.orchestration_approval_token)),
          else:
            from(p in query,
              where: p.orchestration_approval_token == ^patch.orchestration_approval_token
            )

      query |> Repo.update_all(set: [orchestration_approval_token: token])
      token
    else
      make_ref()
    end
  end

  defp defer_approval(patch, reviewer) do
    Bundles.hold_approval(patch, reviewer)
    generation = approval_generation(patch)
    Process.put({:approval_pending, patch.id}, {patch.commit, generation})

    schedule_message(
      self(),
      {:backend_retry, patch.id, patch.commit, reviewer, generation},
      60_000
    )
  end

  defp hold_prerun_approval(project, patch, reviewer) do
    if Process.get(:orchestration_executor, false) or
         BorsNG.MergeBackend.scoped?(project, patch.into_branch) do
      Bundles.hold_approval(patch, reviewer)
      generation = approval_generation(patch)
      Process.put({:approval_pending, patch.id}, {patch.commit, generation})
      {:held_approval, generation}
    else
      reviewer
    end
  end

  defp resolve_prerun_reviewer({:held_approval, generation}, patch) do
    valid =
      if Process.get(:orchestration_executor, false),
        do: patch.orchestration_approval_token == generation,
        else: Process.get({:approval_pending, patch.id}) == {patch.commit, generation}

    if valid,
      do: patch.bundle_reviewer,
      else: nil
  end

  defp resolve_prerun_reviewer(:held_approval, patch), do: patch.bundle_reviewer
  defp resolve_prerun_reviewer(reviewer, _patch), do: reviewer

  defp handle_waiting_preflight(repo_conn, reviewer, patch, try_num, toml \\ nil) do
    prerun_timeout_sec =
      case toml do
        nil -> 30 * 60
        x -> x.prerun_timeout_sec
      end

    prerun_timeout_ms = prerun_timeout_sec * 1000
    elapsed = try_num * @prerun_poll_period

    cond do
      prerun_timeout_sec == 0 ->
        QueueApproval.stop([patch], :preflight_timeout)

        if reviewer == :held_approval or is_tuple(reviewer),
          do: Bundles.drop_held_approval(patch.id)

        send_message(repo_conn, [patch], {:preflight, :timeout})

      elapsed > prerun_timeout_ms ->
        QueueApproval.stop([patch], :preflight_timeout)

        if reviewer == :held_approval or is_tuple(reviewer),
          do: Bundles.drop_held_approval(patch.id)

        send_message(repo_conn, [patch], {:preflight, :timeout})

      true ->
        # Tell the user once, when the poll loop is armed. Later iterations
        # re-poll silently: one comment per minute until the timeout is spam.
        if try_num == 0 do
          send_message(repo_conn, [patch], {:preflight, :waiting})
        end

        Logger.info("Start Poll Patch #{patch.id} prerun")

        schedule_message(
          self(),
          {:prerun_poll, try_num + 1, {reviewer, patch}},
          @prerun_poll_period
        )
    end
  end

  defp check_code_owner(repo_conn, patch, toml) do
    if !toml.use_codeowners do
      true
    else
      Logger.info("Checking code owners")

      code_owner_result =
        try do
          Batcher.GetCodeOwners.get(repo_conn, patch.into_branch)
        rescue
          error ->
            {:error, {:codeowners_exception, error}}
        catch
          kind, reason ->
            {:error, {:codeowners_catch, kind, reason}}
        end

      case code_owner_result do
        {:ok, code_owner} ->
          Logger.info("CODEOWNERS file #{inspect(code_owner)}")

          case GitHub.get_pr_files(repo_conn, patch.pr_xref) do
            {:ok, files} ->
              Logger.info("Files found: #{inspect(files)}")

              required_reviews = BorsNG.CodeOwnerParser.list_required_reviews(code_owner, files)

              passed_review =
                repo_conn
                |> GitHub.get_reviews!(patch.pr_xref)

              Logger.info("Passed reviews: #{inspect(passed_review)}")

              # Convert the list of required reviewers into a list of true/false
              # true indicates that the reviewers requirement was satisfied,
              # false if it is open
              approved_reviews =
                Enum.map(required_reviews, fn x ->
                  # Convert a list of OR reviewers into a true or false
                  Enum.any?(x, fn required ->
                    if String.contains?(required, "/") do
                      # Remove leading @ for team name
                      # Split into org name and team name
                      [org_name, team_name | _] =
                        String.slice(required, 1, String.length(required) - 1)
                        |> String.split("/")

                      # Loop through reviewers, if they on the team accept their approval
                      team_approved =
                        Enum.any?(passed_review["approvers"], fn username ->
                          GitHub.belongs_to_team?(repo_conn, org_name, team_name, username)
                        end)

                      Logger.info("Approved: #{inspect(team_approved)}")
                      team_approved
                    else
                      Enum.any?(passed_review["approvers"], fn username ->
                        String.slice(required, 1, String.length(required) - 1) == username
                      end)
                    end
                  end)
                end)

              code_owner_approval = Enum.reduce(approved_reviews, true, fn x, acc -> x && acc end)

              Logger.info("Approved reviews: #{inspect(approved_reviews)}")
              Logger.info("Code Owner approval: #{inspect(code_owner_approval)}")

              code_owner_approval

            {:error, :get_pr_files, status, _} ->
              Logger.warning(
                "check_code_owner: get_pr_files failed for PR #{patch.pr_xref} with status #{status}"
              )

              false

            {:error, :get_pr_files} ->
              Logger.warning("check_code_owner: get_pr_files failed for PR #{patch.pr_xref}")
              false
          end

        {:error, reason} ->
          Logger.warning(
            "check_code_owner: failed to fetch/parse CODEOWNERS for branch #{patch.into_branch}: #{inspect(reason)}"
          )

          false
      end
    end
  end

  @spec reviews_status(map, Batcher.BorsToml.t()) :: :sufficient | :failed | :insufficient
  defp reviews_status(reviews, toml) do
    failed = Map.fetch!(reviews, "CHANGES_REQUESTED")
    approvals = Map.fetch!(reviews, "APPROVED")

    review_required? = is_integer(toml.required_approvals)
    approvals_needed = (review_required? && toml.required_approvals) || 0
    approved? = approvals >= approvals_needed
    failed? = failed > 0

    cond do
      # NOTE: A way to disable the code reviewing behaviour was requested on #587.
      #   As such, we only apply the reviewing rules if, on bors, the config
      #   `required_approvals` is present and an integer.
      not review_required? ->
        :sufficient

      failed? ->
        :failed

      approved? ->
        :sufficient

      review_required? ->
        :insufficient
    end
  end

  @doc """
  Find a waiting batch to add patches to, or create one. `capacity` is the
  number of patches to insert: 1 for a solo patch, the member count for a
  bundle. A batch is reused only if all patches fit within max_batch_size.
  """
  def get_new_batch(max_batch_size, project_id, into_branch, priority, force) do
    get_new_batch(max_batch_size, project_id, into_branch, priority, force, 1)
  end

  def get_new_batch(_max_batch_size, project_id, into_branch, priority, true, _capacity) do
    {Repo.insert!(Batch.new(project_id, into_branch, priority)), true}
  end

  # A bundle that fills or exceeds max_batch_size cannot share a batch with
  # anything else and cannot be split, so it gets its own batch.
  def get_new_batch(max_batch_size, project_id, into_branch, priority, _force, capacity)
      when is_integer(max_batch_size) and capacity >= max_batch_size do
    {Repo.insert!(Batch.new(project_id, into_branch, priority)), true}
  end

  def get_new_batch(max_batch_size, project_id, into_branch, priority, _force, capacity) do
    Batch
    |> where([b], b.project_id == ^project_id)
    |> where([b], b.state == ^:waiting)
    |> where([b], not b.sealed)
    |> where([b], b.into_branch == ^into_branch)
    |> where([b], b.priority == ^priority)
    |> apply_max_batch_size(max_batch_size, capacity)
    |> order_by([b], desc: b.updated_at)
    |> Repo.all()
    |> Enum.reject(fn b ->
      Repo.all(LinkPatchBatch.from_batch(b.id))
      |> Enum.any?(fn l ->
        Repo.get!(Patch, l.patch_id).is_single
      end)
    end)
    |> Enum.take(1)
    |> case do
      [batch] -> {batch, false}
      _ -> get_new_batch(max_batch_size, project_id, into_branch, priority, true, capacity)
    end
  end

  defp apply_max_batch_size(query, nil, _capacity) do
    query
  end

  defp apply_max_batch_size(query, n, capacity) do
    # The batch may take `capacity` more patches without exceeding the limit.
    room = n - capacity

    query
    |> join(:inner, [b], p in assoc(b, :patches))
    |> having([b, p], count(p.id) <= ^room)
    |> group_by([b], b.id)
  end

  defp raise_batch_priority(%Batch{priority: old_priority} = batch, priority)
       when old_priority < priority do
    project = Repo.get!(Project, batch.project_id)

    batch =
      batch
      |> Batch.changeset_raise_priority(%{priority: priority})
      |> Repo.update!()

    put_incomplete_on_hold(get_repo_conn(project), batch)
  end

  defp raise_batch_priority(_, _) do
    :ok
  end

  defp send_message(repo_conn, patches, message) do
    body = Batcher.Message.generate_message(message, bundle_page_url(patches))

    case body do
      nil ->
        :ok

      _ ->
        Enum.each(patches, fn patch ->
          case post_message(repo_conn, patch, body) do
            :ok ->
              :ok

            err ->
              Logger.warning(
                "send_message: failed to post comment for patch #{patch.id}: #{inspect(err)}"
              )
          end
        end)
    end
  end

  defp post_message(conn, patch, body) do
    if Process.get(:orchestration_executor, false) do
      job = Process.get(:orchestration_job)
      key = "notice:#{job.id}:#{patch.id}:" <> CandidateJournal.digest(body)
      marker = "<!-- bors-outbox #{key} -->"

      Outbox.enqueue(
        patch.project_id,
        "notice",
        {conn, patch.pr_xref, body <> "\n\n" <> marker, marker},
        key
      )

      :ok
    else
      GitHub.post_comment(conn, patch.pr_xref, body)
    end
  end

  # All members of a bundle share a bundle_id, so the first patch's is
  # representative of the group send_message posts to.
  # Tell each sibling pulled out of a batch which member took its bundle with
  # it. One comment per *sibling*, not per cause: a bundle can lose two
  # members at once, and a sibling that hears twice learns nothing the second
  # time. `causes` is ordered, so the named member is deterministic.
  defp send_bundle_pulled(repo_conn, pulled, causes, reason) do
    Enum.each(pulled, fn patch ->
      xref =
        Enum.find_value(causes, fn cause ->
          cause.bundle_id == patch.bundle_id && cause.pr_xref
        end)

      send_message(repo_conn, [patch], {:bundle_pulled, xref, reason})
    end)
  end

  defp bundle_page_url([%Patch{bundle_id: bundle_id} | _]) when not is_nil(bundle_id),
    do: bundle_url(Endpoint, :show, bundle_id)

  defp bundle_page_url(_), do: nil

  defp send_status(
         repo_conn,
         %Batch{id: id, commit: commit},
         message
       ) do
    patches =
      id
      |> Patch.all_for_batch()
      |> Repo.all()

    send_status(repo_conn, id, patches, message)

    unless is_nil(commit) do
      {msg, status} = Batcher.Message.generate_status(message)

      # The tested-commit bors status can be a branch push prerequisite.
      # Retain its synchronous ordering before promotion.
      case GitHub.post_commit_status(
             repo_conn,
             {commit, status, msg, batch_url(Endpoint, :show, id)}
           ) do
        :ok ->
          :ok

        err ->
          Logger.warning(
            "send_status: failed to post commit status for batch #{id}: #{inspect(err)}"
          )
      end
    end
  end

  defp send_status(repo_conn, batch_id, patches, message) do
    {msg, status} = Batcher.Message.generate_status(message)
    batch = Repo.get(Batch, batch_id)
    tested_commit = if batch, do: batch.commit

    Enum.each(patches, fn patch ->
      case post_bors_status(
             repo_conn,
             {patch.commit, status, msg, batch_url(Endpoint, :show, batch_id)},
             batch_id,
             tested_commit
           ) do
        :ok ->
          :ok

        err ->
          Logger.warning(
            "send_status: failed to post commit status for patch #{patch.id}: #{inspect(err)}"
          )
      end
    end)
  end

  defp post_bors_status(conn, data, batch_id, tested_commit) do
    if BorsNG.Worker.Orchestration.Projections.enabled?() and elem(data, 0) != tested_commit do
      job = Process.get(:orchestration_job)

      BorsNG.Worker.Orchestration.Projections.enqueue(
        job.project_id,
        {:status, conn, data, batch_id}
      )

      :ok
    else
      GitHub.post_commit_status(conn, data)
    end
  end

  # this should be called whenever the state of a Batch is changed using Batch.changeset
  @spec send_zulip(%Batch{}, batch_state) :: :ok | :error
  # :waiting | :running | :ok -> do nothing
  defp send_zulip(_, :waiting), do: :ok
  defp send_zulip(_, :running), do: :ok
  defp send_zulip(_, :ok), do: :ok
  # :error | :conflict | :canceled -> send notification
  defp send_zulip(batch, state) do
    {fail_message, pr_messages} =
      try do
        build_message(batch, state)
      rescue
        e ->
          e_message = "Failed to build message:\n#{inspect(e, pretty: true, width: 60)}"
          Logger.error(e_message)
          {"⚠️ bors batch failed!\n\nBatch #{batch.id}: #{state}\n\n#{e_message}", []}
      end

    Zulip.send_message(fail_message)

    pr_messages
    |> Enum.each(&Zulip.send_message/1)
  end

  # see also lib/web/templates/project/show.html.eex
  defp build_message(batch, state) do
    project = Repo.get(Project, batch.project_id)

    project_pr_url =
      Confex.fetch_env!(:bors, :html_github_root) <> "/" <> project.name <> "/pull/"

    statuses = Repo.all(Status.all_for_batch(batch.id))

    statuses_message =
      if Enum.empty?(statuses) do
        ""
      else
        "Status check(s):\n" <>
          (statuses
           |> Enum.map(&format_status/1)
           |> Enum.join("\n"))
      end

    patch_links_pr_xrefs =
      Repo.all(LinkPatchBatch.from_batch(batch.id))
      |> Enum.map(& &1.patch.pr_xref)
      |> Enum.sort()

    num_prs = length(patch_links_pr_xrefs)

    message = """
    ⚠️ `#{project.name}` bors batch [#{batch.id}](#{batch_url(Endpoint, :show, batch.id)}) failed with state: "**#{state}**"!

    #{statuses_message}

    The batch contained the following #{num_prs} PR(s):
    """

    patch_links_pr_xrefs_messages =
      patch_links_pr_xrefs
      |> Enum.with_index(1)
      |> Enum.map(fn {pr_xref, index} ->
        "(#{index}/#{num_prs}) of Batch #{batch.id}: [#{project.name}##{pr_xref}](#{project_pr_url}#{pr_xref})"
      end)

    {message, patch_links_pr_xrefs_messages}
  end

  def format_status(status) do
    if status.url do
      "- [#{status.identifier} (#{BorsNG.BatchView.stringify_state(status.state)})](#{status.url})"
    else
      "- #{status.identifier} (#{BorsNG.BatchView.stringify_state(status.state)})"
    end
  end

  @spec get_repo_conn(%Project{}) :: {{:installation, number}, number}
  defp get_repo_conn(project) do
    Project.installation_connection(project.repo_xref, Repo)
  end

  defp put_incomplete_on_hold(repo_conn, batch) do
    batches =
      batch.project_id
      |> Batch.all_for_project(:running)
      |> where(
        [b],
        b.id != ^batch.id and b.priority < ^batch.priority and ^(not Outbox.enabled?())
      )
      |> Repo.all()

    ids = Enum.map(batches, & &1.id)

    Status
    |> where([s], s.batch_id in ^ids)
    |> Repo.delete_all()

    Enum.each(batches, &send_status(repo_conn, &1, :delayed))

    Batch
    |> where([b], b.id in ^ids)
    |> Repo.update_all(set: [state: :waiting])
  end

  defp schedule_message(pid, message, delay) do
    if Process.get(:orchestration_executor, false) and pid == self() do
      job = Process.get(:orchestration_job)

      key =
        "timer:" <>
          Integer.to_string(job.project_id) <>
          ":" <>
          CandidateJournal.digest(:erlang.term_to_binary(message)) <>
          ":" <> Integer.to_string(Outbox.now() + div(delay + 999, 1000))

      Outbox.enqueue(job.project_id, "info", message, key, Outbox.now() + div(delay + 999, 1000))
      nil
    else
      Process.send_after(pid, message, delay)
    end
  end

  defp poll_after_delay(project) do
    poll_at = (project.batch_delay_sec + 1) * 1000
    schedule_message(self(), {:poll, :once}, poll_at)
  end

  # Validate that there are no duplicate running batchers for the same project
  defp check_self(project_id) do
    if Application.get_env(:bors, :is_test, false) or Process.get(:orchestration_executor, false) do
      :ok
    else
      self = self()
      ^self = BorsNG.Worker.Batcher.Registry.get(project_id)
    end
  end
end
