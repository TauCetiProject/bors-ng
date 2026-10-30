defmodule BorsNG.GitHub.BatchRead do
  @moduledoc "Live PR reads with per-chunk recovery; no cached merge authority."
  require Logger
  alias BorsNG.GitHub

  def enabled?(project),
    do:
      project.name == BorsNG.ProjectConfig.repo() and
        String.trim_trailing(Confex.fetch_env!(:bors, :api_github_root), "/") ==
          "https://api.github.com" and
        Process.get(:orchestration_executor, false)

  def fetch(conn, repository, numbers, mode) when mode in [:identity, :metadata] do
    numbers
    |> Enum.uniq()
    |> Enum.chunk_every(20)
    |> Enum.reduce_while({:ok, %{}}, fn chunk, {:ok, acc} ->
      {result, missing} =
        case GitHub.batch_read(conn, repository, chunk, mode) do
          {:ok, result} ->
            {result, Enum.filter(chunk, &(Map.get(result, &1, :rest) == :rest))}

          error ->
            Logger.warning(
              "batch read fallback mode=#{mode} prs=#{inspect(chunk, charlists: :as_lists)} reason=#{inspect(error)}"
            )

            {%{}, chunk}
        end

      case recover_chunk(conn, missing, mode) do
        {:ok, recovered} -> {:cont, {:ok, acc |> Map.merge(result) |> Map.merge(recovered)}}
        error -> {:halt, error}
      end
    end)
  end

  # Metadata can use the full REST transcription path for just the affected PRs. Identity
  # checks require a fresh, complete REST response; a transport failure never authorizes a merge.
  defp recover_chunk(_conn, numbers, :metadata),
    do: {:ok, Map.new(numbers, &{&1, :rest})}

  defp recover_chunk(conn, numbers, :identity) do
    Enum.reduce_while(numbers, {:ok, %{}}, fn number, {:ok, acc} ->
      case GitHub.read_pr_identity(conn, number) do
        {:ok, %GitHub.Pr{} = pr} ->
          if valid_identity?(pr, conn, number),
            do: {:cont, {:ok, Map.put(acc, number, pr)}},
            else: {:halt, identity_error(number, :invalid_response)}

        error ->
          {:halt, identity_error(number, error)}
      end
    end)
  end

  def identity_error(number, cause),
    do: {:error, %{kind: "live_identity_unavailable", pr: number, cause: inspect(cause)}}

  defp valid_identity?(pr, {_token, repo_id}, number) do
    pr.number == number and pr.base_repo_id == repo_id and
      is_binary(pr.head_sha) and Regex.match?(~r/\A[0-9a-f]{40}\z/, pr.head_sha) and
      is_binary(pr.base_ref) and pr.base_ref != "" and is_boolean(pr.draft) and
      pr.state in [:open, :closed]
  end

  def decode_rest(
        %{
          "number" => number,
          "head" => %{"sha" => head},
          "base" => %{"ref" => base, "repo" => %{"id" => repo_id}},
          "state" => state,
          "draft" => draft
        },
        repo_id,
        number
      )
      when state in ["open", "closed"] do
    pr = %GitHub.Pr{
      number: number,
      head_sha: head,
      base_ref: base,
      base_repo_id: repo_id,
      state: if(state == "open", do: :open, else: :closed),
      draft: draft
    }

    if valid_identity?(pr, {nil, repo_id}, number),
      do: {:ok, pr},
      else: {:error, :identity_response_invalid}
  end

  def decode_rest(_, _, _), do: {:error, :identity_response_invalid}

  def request(repository, numbers, mode) do
    [owner, name] = String.split(repository, "/", parts: 2)
    true = length(numbers) in 1..20 and Enum.all?(numbers, &(is_integer(&1) and &1 > 0))

    metadata =
      if mode == :metadata do
        """
        title body author { __typename login ... on User { databaseId } }
        commits(first:100) { totalCount pageInfo { hasNextPage } nodes { commit { oid author { name email } } } }
        """
      else
        ""
      end

    fields =
      Enum.map_join(numbers, "\n", fn number ->
        "p#{number}:pullRequest(number:#{number}) { number headRefOid baseRefName state isDraft #{metadata} }"
      end)

    %{
      query:
        "query($owner:String!,$name:String!) { repository(owner:$owner,name:$name) { databaseId #{fields} } }",
      variables: %{owner: owner, name: name}
    }
  end

  def decode(%{"data" => %{"repository" => repo}} = response, repo_id, numbers, mode)
      when is_map(repo) do
    cond do
      repo["databaseId"] != repo_id ->
        {:error, :batch_repository_mismatch}

      Enum.any?(Map.get(response, "errors", []), fn error ->
        not match?(["repository", "p" <> _ | _], error["path"])
      end) ->
        {:error, :batch_graphql_error}

      true ->
        failed = Map.get(response, "errors", []) |> Enum.map(&Enum.at(&1["path"], 1))

        {:ok,
         Map.new(numbers, fn number ->
           value =
             if "p#{number}" in failed,
               do: :rest,
               else: decode_pr(repo["p#{number}"], repo_id, number, mode)

           {number, value}
         end)}
    end
  rescue
    _ -> {:error, :batch_response_invalid}
  end

  def decode(_, _, _, _), do: {:error, :batch_response_invalid}

  # A bad alias does not discard its healthy neighbours. The caller must replace every
  # :rest marker with a complete fresh REST read before using identity evidence.
  defp decode_pr(raw, repo_id, number, mode) do
    %{
      "number" => ^number,
      "headRefOid" => head,
      "baseRefName" => base,
      "state" => state,
      "isDraft" => draft
    } = raw

    true = is_binary(head) and Regex.match?(~r/\A[0-9a-f]{40}\z/, head)
    true = is_binary(base) and base != "" and is_boolean(draft)
    true = state in ["OPEN", "CLOSED", "MERGED"]

    pr = %GitHub.Pr{
      number: number,
      head_sha: head,
      base_ref: base,
      base_repo_id: repo_id,
      state: if(state == "OPEN", do: :open, else: :closed),
      draft: draft
    }

    if mode == :identity, do: pr, else: metadata(raw, pr)
  rescue
    _ -> :rest
  end

  defp metadata(raw, pr) do
    # Long histories and non-User authors retain the complete REST path.
    with %{"__typename" => "User", "databaseId" => id, "login" => login} <-
           raw["author"],
         true <- is_integer(id) and is_binary(login),
         %{"totalCount" => count, "pageInfo" => %{"hasNextPage" => false}, "nodes" => nodes} <-
           raw["commits"],
         true <- count in 1..100 and length(nodes) == count,
         %{"commit" => %{"oid" => head}} <- List.last(nodes),
         true <- head == pr.head_sha do
      commits =
        Enum.map(nodes, fn %{
                             "commit" => %{
                               "oid" => sha,
                               "author" => %{"name" => name, "email" => email}
                             }
                           } ->
          true = is_binary(sha) and is_binary(name) and is_binary(email)
          %GitHub.Commit{sha: sha, author_name: name, author_email: email}
        end)

      true = is_binary(raw["title"]) and is_binary(raw["body"])

      %{
        pr: %{
          pr
          | title: raw["title"],
            body: raw["body"],
            user: %GitHub.User{id: id, login: login}
        },
        commits: commits
      }
    else
      _ -> :rest
    end
  rescue
    _ -> :rest
  end
end
