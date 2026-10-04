defmodule BorsNG.Worker.MergeReconciler do
  @moduledoc "Minute heartbeat; hints wake trusted review policy, never grant approval."
  use GenServer
  import Ecto.Query
  require Logger
  alias BorsNG.Database.{Batch, Patch, Project, Repo}
  alias BorsNG.GitHub
  alias BorsNG.Worker.Batcher

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  def tick, do: GenServer.call(__MODULE__, :tick, 55_000)
  def observation, do: :persistent_term.get({__MODULE__, :observation}, nil)

  def init(_) do
    :persistent_term.put({__MODULE__, :observation}, nil)
    {:ok, %{last_dispatch: nil, observation: nil}}
  end

  def handle_call(:tick, _, state) do
    next = reconcile(state)
    :persistent_term.put({__MODULE__, :observation}, next.observation)
    {:reply, next.observation, next}
  end

  defp reconcile(state) do
    case Repo.one(from(p in Project, where: p.name == "TauCetiProject/TauCeti")) do
      nil -> state
      project -> observe(project, state)
    end
  rescue
    error ->
      Logger.warning("merge_reconcile observation failed #{Exception.message(error)}")
      %{state | observation: nil}
  end

  defp observe(project, state) do
    conn = Project.installation_connection(project.repo_xref, Repo)

    with {:ok, snapshot} <- GitHub.merge_backend_snapshot(conn),
         {:ok, candidates} <- GitHub.merge_candidates(conn) do
      batches =
        Repo.all(
          from(b in Batch.all_for_project(project.id, :incomplete),
            where: b.into_branch == "main"
          )
        )

      ready = Enum.filter(candidates, & &1.ready)

      represented =
        Repo.all(
          from(p in Patch,
            join: l in BorsNG.Database.LinkPatchBatch,
            on: l.patch_id == p.id,
            join: b in Batch,
            on: b.id == l.batch_id,
            where:
              b.project_id == ^project.id and b.into_branch == "main" and
                b.state in ^[:waiting, :running],
            select: {p.pr_xref, l.head_sha}
          )
        )
        |> MapSet.new()

      outcomes =
        Repo.all(
          from(l in BorsNG.Database.LinkPatchBatch,
            join: p in assoc(l, :patch),
            join: b in assoc(l, :batch),
            where: b.project_id == ^project.id and b.into_branch == "main" and p.open,
            order_by: [desc: b.id],
            select: {p.pr_xref, l.head_sha, b.state}
          )
        )
        |> Enum.uniq_by(fn {pr, head, _} -> {pr, head} end)

      terminal =
        outcomes
        |> Enum.filter(fn {_, _, status} -> status in [:error, :conflict, :ok] end)
        |> MapSet.new(fn {pr, head, _} -> {pr, head} end)

      holds =
        Repo.all(
          from(p in Patch,
            where:
              p.project_id == ^project.id and p.into_branch == "main" and
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
          eligible: ready,
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
                  p.into_branch == "main" and p.open and not is_nil(p.bundle_reviewer)
            )
          )

        # Starting a missing batcher recovers its holds once. Re-casting r+
        # every minute would duplicate pending preflight loops and comments.
        if held do
          pid = Batcher.Registry.get(project.id)
          send(pid, :recover_backend_holds)
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
      _ -> %{state | observation: nil}
    end
  end
end
