defmodule BorsNG.ApiController do
  @moduledoc """
  JSON API endpoints.
  """

  use BorsNG.Web, :controller

  import Ecto.Query, only: [from: 2]

  alias BorsNG.Database.Repo
  alias BorsNG.Database.Batch
  alias BorsNG.Database.Project

  def project_candidates(conn, %{"id" => id} = params) do
    case Repo.get(Project, id) do
      nil ->
        send_resp(conn, 404, "")

      project ->
        batch_id =
          case Integer.parse(params["batch_id"] || "") do
            {n, ""} when n > 0 -> n
            _ -> nil
          end

        admissions_query =
          from(a in BorsNG.Database.CandidateAdmission,
            where: a.project_id == ^project.id,
            order_by: [desc: a.id],
            limit: 500
          )

        admissions_query =
          if batch_id,
            do: from(a in admissions_query, where: a.batch_id == ^batch_id),
            else: admissions_query

        admissions_query =
          if params["base"],
            do: from(a in admissions_query, where: a.into_branch == ^params["base"]),
            else: admissions_query

        admissions =
          Repo.all(admissions_query)
          |> Enum.map(&Map.take(&1, [:id, :batch_id, :link_id, :observed_at, :snapshot]))

        conn
        |> put_resp_header("cache-control", "no-store")
        |> json(%{
          schema: "tauceti-bors.candidates/v1",
          repo: project.name,
          base: params["base"],
          batch_id: batch_id,
          candidates:
            BorsNG.Worker.CandidateJournal.diagnostics(project.id, params["base"], batch_id),
          admissions: admissions,
          historical_unknown_reason: "no journal backfill; absent facts are unknown",
          timestamp_unit: "unix_microseconds",
          queue_authority: false
        })
    end
  end

  @doc """
  GET /api/active-batches

  Returns the IDs of all active (incomplete) batches across all projects.
  """
  def active_batches(conn, _params) do
    ids =
      from(b in Batch,
        where: b.state == ^:waiting or b.state == ^:running,
        select: b.id
      )
      |> Repo.all()

    render(conn, "active_batches.json", batch_ids: ids)
  end

  @doc """
  GET /repositories/:id/active-batches

  Returns the IDs of all active (incomplete) batches for a specific project.
  Responds with 404 if the project does not exist.
  """
  def project_active_batches(conn, %{"id" => id} = params) do
    case Repo.get(Project, id) do
      nil ->
        send_resp(conn, 404, "")

      %Project{} = project ->
        base = params["base"]
        query = Batch.all_for_project(project.id)
        query = if base, do: from(b in query, where: b.into_branch == ^base), else: query

        active =
          Repo.all(
            from(b in query, where: b.state in ^[:waiting, :running], order_by: [desc: b.id])
          )

        # Only open PR history is relevant; one joined query avoids scanning
        # every old closed batch as the repository grows.
        outcomes_query =
          from(l in BorsNG.Database.LinkPatchBatch,
            join: p in assoc(l, :patch),
            join: b in assoc(l, :batch),
            where: p.open and b.project_id == ^project.id,
            order_by: [desc: b.id],
            select: %{pr: p.pr_xref, head_sha: l.head_sha, batch_id: b.id, state: b.state}
          )

        outcomes_query =
          if base,
            do: from([l, p, b] in outcomes_query, where: b.into_branch == ^base),
            else: outcomes_query

        outcomes = Repo.all(outcomes_query) |> Enum.uniq_by(&{&1.pr, &1.head_sha})

        held_query =
          from(p in BorsNG.Database.Patch,
            where:
              p.project_id == ^project.id and
                p.open and not is_nil(p.bundle_reviewer)
          )

        held_query =
          if base, do: from(p in held_query, where: p.into_branch == ^base), else: held_query

        held = Repo.all(held_query) |> Enum.map(&%{pr: &1.pr_xref, head_sha: &1.commit})

        approvals =
          Repo.all(
            from(p in BorsNG.Database.Patch,
              where: p.project_id == ^project.id and not is_nil(p.manual_approval)
            )
          )
          |> Enum.filter(&BorsNG.Worker.ManualApproval.valid?/1)
          |> Enum.map(&Map.put(&1.manual_approval, "pr", &1.pr_xref))

        details = Enum.map(active, &batch_detail/1)

        requested =
          case Integer.parse(params["batch_id"] || "") do
            {batch_id, ""} when batch_id > 0 ->
              case Repo.get(Batch, batch_id) do
                %Batch{project_id: project_id} = batch when project_id == project.id ->
                  if is_nil(base) or batch.into_branch == base, do: batch_detail(batch), else: nil

                _ ->
                  nil
              end

            _ ->
              nil
          end

        conn
        |> put_resp_header("cache-control", "no-store")
        |> json(%{
          schema: "tauceti-bors.observation/v1",
          repo: project.name,
          base: base,
          batch_ids: Enum.map(active, & &1.id),
          batches: details,
          held: held,
          bump_repairs: BorsNG.Worker.BumpRecovery.repair_candidates(project.id, base),
          outcomes: outcomes,
          scope_approvals: approvals,
          requested_batch: requested,
          handoff:
            if(project.name == BorsNG.ProjectConfig.repo(),
              do: BorsNG.Worker.MergeReconciler.observation(),
              else: nil
            )
        })
    end
  end

  defp batch_detail(batch) do
    members =
      Repo.all(BorsNG.Database.LinkPatchBatch.from_batch(batch.id))
      |> Enum.map(&%{pr: &1.patch.pr_xref, head_sha: &1.head_sha})

    %{
      id: batch.id,
      base: batch.into_branch,
      state: batch.state,
      sealed: batch.sealed,
      head_sha: batch.commit,
      base_sha: batch.base_sha,
      promotion_retry_at: batch.promotion_retry_at,
      promotion_failures: batch.promotion_failures,
      recovery_after: batch.recovery_after,
      recovery_failures: batch.recovery_failures,
      train: train_detail(batch),
      members: members
    }
  end

  defp train_detail(batch) do
    candidate = BorsNG.Worker.CandidateJournal.current(batch)

    if BorsNG.Worker.SpeculativeTrain.tracked?(candidate) do
      %{
        candidate_id: candidate.id,
        generation: candidate.generation,
        staging_ref: BorsNG.Worker.SpeculativeTrain.ref(candidate),
        predecessor_id: candidate.snapshot["speculative_predecessor_id"]
      }
    end
  end
end
