defmodule BorsNG.ProjectConfig do
  @moduledoc "Runtime binding for one downstream deployment; legacy Tau Ceti defaults remain unchanged."

  def repo, do: System.get_env("BORS_PROJECT", "TauCetiProject/TauCeti")
  def branch, do: System.get_env("BORS_BASE_BRANCH", "main")
  def scoped?(%{name: name}, base), do: name == repo() and base == branch()
  def scoped?(_, _), do: false

  def integer(name, legacy) do
    default = if repo() == "TauCetiProject/TauCeti", do: legacy, else: "0"

    case Integer.parse(System.get_env(name, to_string(default))) do
      {value, ""} when value > 0 -> value
      _ -> 0
    end
  end

  def reviewer, do: System.get_env("BORS_REVIEW_LOGIN", if(repo() == "TauCetiProject/TauCeti", do: "tauceti-review-bot[bot]", else: ""))

  def repo_id, do: integer("BORS_REPOSITORY_ID", 1_256_881_326)
  def actor_id, do: integer("BORS_ACTOR_ID", 337_246_530)
  def cancellation_workflow_id, do: integer("BORS_CANCELLATION_WORKFLOW_ID", 288_851_159)
end
