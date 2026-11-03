defmodule BorsNG.Worker.Batcher.DeferredConflicts do
  @moduledoc "Classify deferred conflicts against actual main through the existing repair handoff."
  import Ecto.Query

  alias BorsNG.Database.{
    Batch,
    Candidate,
    CandidateAdmission,
    LinkPatchBatch,
    Patch,
    Project,
    Repo
  }

  alias BorsNG.GitHub
  alias BorsNG.Worker.{Batcher, CandidateJournal, LocalGit, SourceCompatibility}
  alias BorsNG.Worker.Orchestration.Outbox

  # Ordinary outbox work runs after dispatch, in four-head turns. This also
  # discovers unclassified deferrals created before this code was deployed.
  def wake(project) do
    if project && project.name == BorsNG.ProjectConfig.repo() && Outbox.enabled?() &&
         SourceCompatibility.enabled?() do
      query =
        from(b in Batch,
          join: l in LinkPatchBatch,
          on: l.batch_id == b.id,
          join: a in CandidateAdmission,
          on: a.link_id == l.id,
          left_join: c in Candidate,
          on: c.batch_id == b.id,
          where:
            b.project_id == ^project.id and b.state == :waiting and b.sealed and
              is_nil(b.commit) and is_nil(c.id) and
              a.snapshot["retry_reason"] == "merge_conflict_deferred",
          distinct: true,
          order_by: b.id,
          limit: 4,
          select: b.id
        )

      cursor_key = {__MODULE__, project.id}
      cursor = Process.get(cursor_key, 0)
      newer = Repo.all(from(b in query, where: b.id > ^cursor))
      batches = if newer == [], do: Repo.all(query), else: newer
      if batches != [], do: Process.put(cursor_key, List.last(batches))

      for id <- batches do
        Repo.all(LinkPatchBatch.from_batch(id))
        |> Enum.filter(&is_nil(&1.patch.bundle_id))
        |> Enum.sort_by(& &1.patch.pr_xref)
        |> Enum.map(&%{patch_id: &1.patch_id, head_sha: &1.head_sha})
        |> Enum.chunk_every(4)
        |> Enum.each(fn members ->
          Outbox.enqueue(
            project.id,
            "info",
            {:triage_deferred_conflicts, id, members},
            "conflict-triage:#{id}:#{CandidateJournal.digest(members)}"
          )
        end)
      end
    end
  end

  def classify(
        project_id,
        batch_id,
        members,
        checker \\ &LocalGit.compatibility(&1, &2, &3, &4, 8)
      ) do
    project = Repo.get!(Project, project_id)
    conn = Project.installation_connection(project.repo_xref, Repo)
    batch = Repo.get(Batch, batch_id)

    if project.name == BorsNG.ProjectConfig.repo() and SourceCompatibility.enabled?() and
         fresh_waiting?(batch, project_id) do
      links = matching_links(batch_id, members)

      case GitHub.get_branch(conn, BorsNG.ProjectConfig.branch()) do
        {:ok, %{commit: main}} ->
          SourceCompatibility.check(project, conn, main, Enum.map(links, & &1.patch), checker)

          # The assembly base may include an unpromoted parent. Only current
          # main can establish the independent repair result used here.
          with {:ok, %{commit: ^main}} <- GitHub.get_branch(conn, BorsNG.ProjectConfig.branch()),
               true <- fresh_waiting?(Repo.get(Batch, batch_id), project_id) do
            for link <- matching_links(batch_id, members) do
              p = link.patch
              r = p.source_compatibility || %{}

              if r["head_sha"] == link.head_sha and r["base_sha"] == main and
                   r["policy"] == "local-source-v3" and
                   r["status"] in ["conflict", "missing_source"] and
                   SourceCompatibility.requested?(p, link.head_sha) and
                   not Outbox.withdrawal_pending?(project_id, [p.id]) do
                Batcher.do_handle_cast({:cancel, p.id, :source_compatibility}, project_id)
              end
            end

            reopen_pool(batch_id, project_id, main)
          else
            _ -> :ok
          end

        _ ->
          throw({:live_members_unavailable, :conflict_main_unavailable})
      end
    end

    :ok
  end

  defp fresh_waiting?(nil, _), do: false

  defp fresh_waiting?(batch, project_id),
    do:
      batch.project_id == project_id and batch.into_branch == BorsNG.ProjectConfig.branch() and
        batch.state == :waiting and is_nil(batch.commit) and
        is_nil(CandidateJournal.latest(batch.id))

  defp matching_links(batch_id, members) do
    expected = MapSet.new(members, &{&1.patch_id, &1.head_sha})

    Repo.all(LinkPatchBatch.from_batch(batch_id))
    |> Enum.filter(
      &(MapSet.member?(expected, {&1.patch_id, &1.head_sha}) and
          &1.head_sha == &1.patch.commit and is_nil(&1.patch.bundle_id) and
          &1.patch.open and not &1.patch.is_draft)
    )
  end

  defp reopen_pool(batch_id, project_id, main) do
    Repo.transaction(fn ->
      batch = Repo.one(from(b in Batch, where: b.id == ^batch_id, lock: "FOR UPDATE"))

      if fresh_waiting?(batch, project_id) do
        links = Repo.all(LinkPatchBatch.from_batch(batch_id))
        ids = Enum.map(links, & &1.patch_id)
        Repo.all(from(p in Patch, where: p.id in ^ids, order_by: p.id, lock: "FOR UPDATE"))
        links = Repo.all(LinkPatchBatch.from_batch(batch_id))

        ready =
          links != [] and
            Enum.all?(links, fn l ->
              p = l.patch
              r = p.source_compatibility || %{}

              not p.is_single and p.open and not p.is_draft and l.head_sha == p.commit and
                l.manual_approval == p.manual_approval and
                (not is_nil(p.bundle_id) or
                   (r["head_sha"] == l.head_sha and r["base_sha"] == main and
                      r["policy"] == "local-source-v3" and
                      r["status"] in ["pass", "unknown"] and
                      not SourceCompatibility.requested?(p, l.head_sha) and
                      r["hold_head"] != l.head_sha))
            end)

        if ready and not Outbox.withdrawal_pending?(project_id, ids),
          do: batch |> Batch.changeset(%{sealed: false}) |> Repo.update!()
      end
    end)
  end
end
