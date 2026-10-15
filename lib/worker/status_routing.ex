defmodule BorsNG.Worker.StatusRouting do
  @moduledoc "Avoid serialized no-op status work while preserving construction races."
  import Ecto.Query
  alias BorsNG.Database.{Batch, Candidate, Repo}
  alias BorsNG.Worker.Orchestration.Outbox

  def batch?(project_id, commit) do
    # Legacy publication can precede the commit-row write. The async lane
    # durably binds before publication, but conservatively retain observations
    # while a candidate is being assembled on temporary refs as well.
    not Outbox.enabled?() or
      Repo.exists?(from(b in Batch, where: b.project_id == ^project_id and b.commit == ^commit)) or
      Repo.exists?(
        from(b in Batch,
          join: c in Candidate,
          on: c.batch_id == b.id,
          where: b.project_id == ^project_id and b.state == :waiting
        )
      )
  end

  def filtered(project_id) do
    :telemetry.execute([:bors, :webhook, :status_filtered], %{count: 1}, %{project_id: project_id})
  end
end
