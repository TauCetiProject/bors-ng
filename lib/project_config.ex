defmodule BorsNG.ProjectConfig do
  @moduledoc "Runtime binding for one downstream deployment; legacy Tau Ceti defaults remain unchanged."

  def repo, do: System.get_env("BORS_PROJECT", "TauCetiProject/TauCeti")
  def branch, do: System.get_env("BORS_BASE_BRANCH", "main")
  def scoped?(%{name: name}, base), do: name == repo() and base == branch()
  def scoped?(_, _), do: false

  def reviewer,
    do:
      System.get_env(
        "BORS_REVIEW_LOGIN",
        if(repo() == "TauCetiProject/TauCeti", do: "tauceti-review-bot[bot]", else: "")
      )
end
