defmodule BorsNG.GitHub.BatchRead do
  @moduledoc "Bounded live PR reads; no statuses, approval decisions or mutable read cache."
  alias BorsNG.GitHub

  def enabled?(project),
    do:
      project.name == "TauCetiProject/TauCeti" and
        String.trim_trailing(Confex.fetch_env!(:bors, :api_github_root), "/") ==
          "https://api.github.com" and
        Process.get(:orchestration_executor, false)

  def fetch(
        conn,
        repository,
        numbers,
        mode,
        deadline \\ System.monotonic_time(:millisecond) + 8_000
      )
      when mode in [:identity, :metadata] do
    numbers
    |> Enum.uniq()
    |> Enum.chunk_every(20)
    |> Enum.reduce_while({:ok, %{}}, fn chunk, {:ok, acc} ->
      case GitHub.batch_read_bounded(conn, repository, chunk, mode, deadline) do
        {:ok, result} -> {:cont, {:ok, Map.merge(acc, result)}}
        _ -> {:halt, {:error, :batch_read_unavailable}}
      end
    end)
  end

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
    with false <- Map.has_key?(response, "errors"),
         ^repo_id <- repo["databaseId"] do
      Map.new(numbers, fn number ->
        %{
          "number" => ^number,
          "headRefOid" => head,
          "baseRefName" => base,
          "state" => state,
          "isDraft" => draft
        } = raw = Map.fetch!(repo, "p#{number}")

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

        value = if mode == :identity, do: pr, else: metadata(raw, pr)
        {number, value}
      end)
      |> then(&{:ok, &1})
    else
      _ -> {:error, :batch_read_unavailable}
    end
  rescue
    _ -> {:error, :batch_read_unavailable}
  end

  def decode(_, _, _, _), do: {:error, :batch_read_unavailable}

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
