defmodule BorsNG.Worker.MergeReconciler do
  @moduledoc "Minute heartbeat; recover App-owned eligibility and wake the shared policy."
  use GenServer
  import Ecto.Query
  require Logger
  alias BorsNG.Database.{Batch, Patch, Project, Repo, User}
  alias BorsNG.GitHub
  alias BorsNG.Worker.Batcher

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  def tick, do: GenServer.call(__MODULE__, :tick, 55_000)
  def observation, do: :persistent_term.get({__MODULE__, :observation}, nil)

  def init(_) do
    :persistent_term.put({__MODULE__, :observation}, nil)

    {:ok,
     %{
       last_dispatch: nil,
       observation: nil,
       candidate_cursor: nil,
       candidate_scan: %{},
       candidate_snapshot: [],
       candidate_completed_at: nil
     }}
  end

  def handle_call(:tick, _, state) do
    next = reconcile(state)
    :persistent_term.put({__MODULE__, :observation}, next.observation)
    {:reply, next.observation, next}
  end

  defp reconcile(state) do
    configured_repo = BorsNG.ProjectConfig.repo()

    case Repo.one(from(p in Project, where: p.name == ^configured_repo)) do
      nil -> state
      project -> observe(project, state)
    end
  rescue
    error ->
      Logger.warning("merge_reconcile observation failed #{Exception.message(error)}")
      %{state | observation: nil}
  end

  @doc false
  def wake_idle_queue(pid, %{backend: "bors", github_count: 0}, batches) do
    if Enum.any?(batches, &(&1.state == :waiting)) and
         not Enum.any?(batches, &(&1.state == :running)) do
      Batcher.poll(pid)
      :woken
    else
      :idle
    end
  end

  def wake_idle_queue(_pid, _snapshot, _batches), do: :idle

  defp observe(project, state) do
    BorsNG.Worker.SourceCompatibility.wake(project)
    send(Batcher.Registry.get(project.id), :recover_queue)
    conn = Project.installation_connection(project.repo_xref, Repo)
    deadline = System.monotonic_time(:millisecond) + 40_000
    {bump, state} = offer_bump(project, state)
    offer_known_candidates(project, bump)

    with {:ok, before_handoff} <-
           bounded_read(deadline, 15_000, &GitHub.merge_backend_snapshot(conn, &1)),
         released <- BorsNG.Worker.QueueHandoff.trim(conn, before_handoff, deadline),
         {:ok, snapshot} <- handoff_snapshot(conn, before_handoff, deadline),
         {:ok, candidates, state} <- candidate_page(conn, state, deadline) do
      offer_missing_candidates(project, candidates)

      batches =
        Repo.all(
          from(b in Batch.all_for_project(project.id, :incomplete),
            where: b.into_branch == ^BorsNG.ProjectConfig.branch()
          )
        )

      # Cancellation or a restart can leave waiting work with only the long CI poll.
      # Reuse this heartbeat to ask the serialized batcher to reconsider its queue.
      # The normal start guard still rechecks the backend and every member.
      wake_idle_queue(Batcher.Registry.get(project.id), snapshot, batches)

      ready = Enum.filter(candidates, & &1.ready)

      represented =
        Repo.all(
          from(p in Patch,
            join: l in BorsNG.Database.LinkPatchBatch,
            on: l.patch_id == p.id,
            join: b in Batch,
            on: b.id == l.batch_id,
            where:
              b.project_id == ^project.id and b.into_branch == ^BorsNG.ProjectConfig.branch() and
                b.state in ^[:waiting, :running],
            select: {p.pr_xref, l.head_sha}
          )
        )
        |> MapSet.new()

      latest =
        from(l in BorsNG.Database.LinkPatchBatch,
          join: p in Patch,
          on: p.id == l.patch_id,
          where:
            p.project_id == ^project.id and p.open and not p.queue_pending and
              l.head_sha == p.commit,
          group_by: [p.id, l.head_sha],
          select: %{patch_id: p.id, head_sha: l.head_sha, batch_id: max(l.batch_id)}
        )

      outcomes =
        Repo.all(
          from(l in subquery(latest),
            join: p in Patch,
            on: p.id == l.patch_id,
            join: b in Batch,
            on: b.id == l.batch_id,
            where: b.into_branch == ^BorsNG.ProjectConfig.branch(),
            select: {p.pr_xref, l.head_sha, b.state}
          )
        )

      terminal =
        outcomes
        |> Enum.filter(fn {_, _, status} -> status in [:error, :conflict, :ok] end)
        |> MapSet.new(fn {pr, head, _} -> {pr, head} end)

      holds =
        Repo.all(
          from(p in Patch,
            where:
              p.project_id == ^project.id and p.into_branch == ^BorsNG.ProjectConfig.branch() and
                p.open and not is_nil(p.bundle_reviewer),
            select: {p.pr_xref, p.commit}
          )
        )
        |> MapSet.new()

      pending =
        Enum.filter(ready, fn p ->
          if snapshot.backend == "queue",
            do: not p.queued,
            else:
              not MapSet.member?(represented, {p.pr, p.head_sha}) and
                not MapSet.member?(terminal, {p.pr, p.head_sha}) and
                not MapSet.member?(holds, {p.pr, p.head_sha})
        end)

      other_count =
        if snapshot.backend == "queue", do: length(batches), else: snapshot.github_count

      observation =
        Map.merge(snapshot, %{
          observed_at: DateTime.to_iso8601(DateTime.utc_now()),
          bors_count: length(batches),
          released_github_prs: Enum.map(released, & &1.pr),
          eligible: ready,
          candidate_snapshot_at: state.candidate_completed_at,
          candidate_scan_complete: is_nil(state.candidate_cursor),
          pending: length(pending),
          overlap: snapshot.github_count > 0 and batches != [],
          reason: if(other_count == 0, do: "drained", else: "outgoing_not_drained")
        })

      Logger.info(Jason.encode!(Map.put(observation, :schema, "tauceti-merge.observation/v1")))
      state = %{state | observation: observation}
      now = System.monotonic_time(:second)
      due = is_nil(state.last_dispatch) or now - state.last_dispatch >= 300

      signature =
        {snapshot.backend, snapshot.updated_at, snapshot.github_count, length(batches),
         Enum.map(pending, &{&1.pr, &1.head_sha}) |> Enum.sort()}

      changed = Map.get(state, :last_wake_signature) != signature

      # Recover manual as well as review-bot holds after restarts. The
      # admission guard and preflight still recheck each held approval.
      if other_count == 0 and snapshot.backend == "bors" do
        held =
          Repo.exists?(
            from(p in Patch,
              where:
                p.project_id == ^project.id and
                  p.into_branch == ^BorsNG.ProjectConfig.branch() and p.open and
                  not is_nil(p.bundle_reviewer)
            )
          )

        # Wake dormant transferred holds without one API retry loop per PR.
        # The batcher tracks each head's pending preflight generation.
        if held do
          pid = Batcher.Registry.get(project.id)
          send(pid, :wake_backend_holds)
        end
      end

      if other_count == 0 and pending != [] and due and changed do
        case GitHub.dispatch_reconcile(conn) do
          :ok -> Map.merge(state, %{last_dispatch: now, last_wake_signature: signature})
          _ -> state
        end
      else
        state
      end
    else
      {:candidate_error, next} -> %{next | observation: nil}
      _ -> %{state | observation: nil}
    end
  end

  # Discovery can restart or fail independently of recovery. Select from the
  # existing patch inventory so an incomplete GitHub page never rewinds fairness.
  # Offering a recheck grants no approval and needs no backend observation.
  defp offer_known_candidates(project, bump) do
    cursor = project.eligibility_cursor || 0
    limit = if bump, do: 3, else: 4
    excluded = if bump, do: bump.pr, else: 0

    query =
      from(p in Patch,
        where: p.project_id == ^project.id and p.open and p.pr_xref != ^excluded,
        order_by: p.pr_xref,
        select: p.pr_xref
      )

    newer = Repo.all(from(p in query, where: p.pr_xref > ^cursor, limit: ^limit))
    remaining = limit - length(newer)

    older =
      if remaining > 0,
        do: Repo.all(from(p in query, where: p.pr_xref <= ^cursor, limit: ^remaining)),
        else: []

    offered = Enum.filter(newer ++ older, &offer_eligibility(project.id, &1))

    case List.last(offered) do
      nil ->
        :ok

      last ->
        # A failed checkpoint only repeats reads; it cannot suppress ordinary
        # admission. The next heartbeat rereads the durable position.
        checkpoint(project.id, last)
    end
  rescue
    error ->
      Logger.warning("eligibility scan deferred #{inspect(error.__struct__)}")
      :ok
  end

  defp checkpoint(project_id, cursor) do
    Repo.update_all(from(p in Project, where: p.id == ^project_id),
      set: [eligibility_cursor: cursor]
    )
  rescue
    error -> Logger.warning("eligibility checkpoint deferred #{inspect(error.__struct__)}")
  end

  # An opened/reopened webhook can be missed. Discovery repairs that inventory
  # gap through the same exact-head observer; known PRs use the durable scan.
  # This bounded catch-up is normally empty and grants no admission itself.
  defp offer_missing_candidates(project, candidates) do
    known =
      Repo.all(from(p in Patch, where: p.project_id == ^project.id and p.open, select: p.pr_xref))
      |> MapSet.new()

    candidates
    |> Enum.reject(&MapSet.member?(known, &1.pr))
    |> Enum.take(4)
    |> Enum.each(&offer_eligibility(project.id, &1.pr))
  end

  defp offer_eligibility(project_id, pr) do
    Batcher.eligibility(Batcher.Registry.get(project_id), pr)
    true
  catch
    :exit, _ ->
      Logger.warning("eligibility wake deferred pr=#{pr}")
      false
  end

  # Offer the known dependency bump even when GitHub candidate discovery fails.
  # One slot per five minutes, inside the existing four-candidate heartbeat budget.
  @doc false
  def offer_bump(project, state, now \\ System.monotonic_time(:second)) do
    if is_nil(Map.get(state, :last_bump_offer)) or now - state.last_bump_offer >= 300 do
      query =
        from(p in Patch,
          join: u in User,
          on: u.id == p.author_id,
          where:
            u.login == ^BorsNG.ProjectConfig.reviewer() and p.project_id == ^project.id and
              p.into_branch == ^BorsNG.ProjectConfig.branch() and p.open and not p.is_draft and
              not p.queue_pending and
              is_nil(p.bundle_reviewer) and is_nil(p.bundle_id) and is_nil(p.manual_approval) and
              (like(p.head_ref, "bump-mathlib/%") or p.head_ref == "hopscotch/lkg-bump"),
          order_by: p.id,
          limit: 1,
          select: %{pr: p.pr_xref, id: p.id}
        )

      cursor = Map.get(state, :bump_cursor, 0)
      bump = Repo.one(from(p in query, where: p.id > ^cursor)) || Repo.one(query)

      if bump do
        if offer_eligibility(project.id, bump.pr),
          do: {bump, Map.merge(state, %{last_bump_offer: now, bump_cursor: bump.id})},
          else: {nil, state}
      else
        {nil, state}
      end
    else
      {nil, state}
    end
  end

  defp handoff_snapshot(conn, %{backend: "bors", github_count: count}, deadline) when count > 0,
    do: bounded_read(deadline, 15_000, &GitHub.merge_backend_snapshot(conn, &1))

  defp handoff_snapshot(_, snapshot, _), do: {:ok, snapshot}

  defp bounded_read(deadline, cap, read) do
    remaining = deadline - System.monotonic_time(:millisecond)
    if remaining > 0, do: read.(min(cap, remaining)), else: {:error, :heartbeat_deadline}
  end

  # One GraphQL page per heartbeat. Preserve the last complete observation
  # while refreshing it incrementally and expose its age and scan coverage.
  defp candidate_page(conn, state, deadline) do
    with {:ok, page} <-
           bounded_read(
             deadline,
             15_000,
             &GitHub.merge_candidates_page(conn, Map.get(state, :candidate_cursor), &1)
           ) do
      scan = if is_nil(Map.get(state, :candidate_cursor)), do: %{}, else: state.candidate_scan
      scan = Enum.reduce(page.candidates, scan, &Map.put(&2, &1.pr, &1))

      if is_nil(page.next_cursor) do
        candidates = scan |> Map.values() |> Enum.sort_by(& &1.pr)

        next =
          Map.merge(state, %{
            candidate_cursor: nil,
            candidate_scan: %{},
            candidate_snapshot: candidates,
            candidate_completed_at: DateTime.to_iso8601(DateTime.utc_now())
          })

        {:ok, candidates, next}
      else
        next =
          Map.merge(state, %{
            candidate_cursor: page.next_cursor,
            candidate_scan: scan,
            candidate_completed_at: Map.get(state, :candidate_completed_at)
          })

        candidates =
          case Map.get(state, :candidate_completed_at) do
            nil -> Map.values(scan)
            _ -> state.candidate_snapshot
          end

        {:ok, candidates, next}
      end
    else
      _ -> {:candidate_error, Map.merge(state, %{candidate_cursor: nil, candidate_scan: %{}})}
    end
  end
end
