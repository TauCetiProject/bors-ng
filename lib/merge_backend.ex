defmodule BorsNG.MergeBackend do
  @moduledoc "Live, observational handoff for Tau Ceti main; no ownership record."
  require Logger
  alias BorsNG.Database.{Project, Repo}
  alias BorsNG.GitHub

  def scoped?(project, base), do: project.name == "TauCetiProject/TauCeti" and base == "main"

  # Starting an existing batch (including a bisection child) must not require
  # bors to remain selected: that would strand the outgoing queue forever.
  def allow(project, base, purpose) do
    if scoped?(project, base) do
      conn = Project.installation_connection(project.repo_xref, Repo)
      result = decide(GitHub.merge_backend_snapshot(conn), purpose)
      Logger.info("merge_backend #{inspect(result)} purpose=#{purpose} project=#{project.name}")
      result
    else
      :ok
    end
  end

  def decide({:ok, %{backend: backend, github_count: 0}}, :admit) when backend == "bors",
    do: :ok

  def decide({:ok, %{backend: backend, github_count: 0}}, :start)
      when backend in ["queue", "bors"],
      do: :ok

  def decide({:ok, %{github_count: n}}, _) when is_integer(n) and n > 0,
    do: {:defer, :github_not_drained}

  def decide({:ok, _}, _), do: {:defer, :backend_not_selected}
  def decide(_, _), do: {:defer, :observation_unavailable}
end
