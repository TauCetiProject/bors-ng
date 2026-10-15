defmodule BorsNG.Worker.StatusRouting do
  @moduledoc "Avoid serialized no-op status work while preserving construction races."
  import Ecto.Query
  alias BorsNG.Database.{Batch, Candidate, CandidateEvent, Repo}
  alias BorsNG.Worker.Orchestration.Outbox

  def batch?(project_id, commit) do
    # Legacy publication can precede the commit-row write. The async lane
    # durably binds before publication, but conservatively retain observations
    # while a candidate is being assembled on temporary refs as well.
    latest =
      from(c in Candidate,
        group_by: c.batch_id,
        select: %{batch_id: c.batch_id, id: max(c.id)}
      )

    ended_or_local =
      from(e in CandidateEvent,
        where: e.kind in ["constructed", "construction_stopped", "local_assembly_selected"],
        select: e.candidate_id
      )

    not Outbox.enabled?() or
      Repo.exists?(from(b in Batch, where: b.project_id == ^project_id and b.commit == ^commit)) or
      Repo.exists?(
        from(b in Batch,
          join: c in Candidate,
          on: c.batch_id == b.id,
          join: latest in subquery(latest),
          on: latest.id == c.id,
          where:
            b.project_id == ^project_id and b.state == :waiting and
              c.id not in subquery(ended_or_local)
        )
      )
  end

  def filtered(project_id) do
    :telemetry.execute([:bors, :webhook, :status_filtered], %{count: 1}, %{project_id: project_id})
  end
end
