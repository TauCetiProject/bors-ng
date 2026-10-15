defmodule BorsNG.Worker.StatusRouting do
  @moduledoc "Avoid serialized no-op status work while preserving construction races."
  import Ecto.Query
  alias BorsNG.Database.{Attempt, Batch, Patch, Repo}
  alias BorsNG.Worker.Orchestration.Outbox

  def batch?(project_id, commit) do
    # Legacy publication can precede the commit-row write. The async lane
    # durably binds before publication, but conservatively retain observations
    # while a candidate is being assembled on temporary refs as well.
    not Outbox.enabled?() or
      Repo.exists?(
        from(b in Batch,
          where:
            b.project_id == ^project_id and
              (b.commit == ^commit or (b.state == :waiting and not is_nil(b.candidate_id)))
        )
      ) or
      Repo.exists?(
        from(a in Attempt,
          join: p in Patch,
          on: p.id == a.patch_id,
          where: p.project_id == ^project_id and a.commit == ^commit
        )
      )
  end

  def filtered(project_id) do
    :telemetry.execute([:bors, :webhook, :status_filtered], %{count: 1}, %{project_id: project_id})
  end
end
