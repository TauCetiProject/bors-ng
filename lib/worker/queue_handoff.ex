defmodule BorsNG.Worker.QueueHandoff do
  @moduledoc "Release unstarted outgoing work; the existing guards drain active builds."
  require Logger
  alias BorsNG.GitHub

  # Each removal rereads the setting and the current last entry. Removing from
  # the back cannot change the inputs of the builds ahead of it. Limit both
  # requests and elapsed time so the observation heartbeat remains responsive.
  def trim(conn, snapshot, read_deadline \\ nil)

  def trim(conn, %{backend: "bors", github_count: count, updated_at: selected_at}, read_deadline)
      when count > 0 do
    deadline =
      min(
        read_deadline || System.monotonic_time(:millisecond) + 10_000,
        System.monotonic_time(:millisecond) + 10_000
      )

    Enum.reduce_while(1..16, [], fn _, removed ->
      if System.monotonic_time(:millisecond) >= deadline do
        {:halt, removed}
      else
        case GitHub.release_queue_tail(conn, selected_at, deadline) do
          {:ok, entry} ->
            {:cont, [entry | removed]}

          {:error, reason} ->
            Logger.warning("queue_handoff deferred reason=#{inspect(reason)}")
            {:halt, removed}

          _ ->
            {:halt, removed}
        end
      end
    end)
    |> Enum.reverse()
  end

  def trim(_, _, _), do: []

  @doc "Only a complete observation of an unbuilt last entry permits removal."
  def queued_tail(%{
        "totalCount" => 0,
        "nodes" => []
      }),
      do: {:done, :empty}

  def queued_tail(%{
        "totalCount" => count,
        "nodes" => [
          %{
            "id" => entry_id,
            "position" => position,
            "state" => state,
            "headCommit" => head,
            "pullRequest" => %{"id" => id, "number" => pr, "headRefOid" => sha}
          }
        ]
      })
      when is_integer(count) and count > 0 and position == count and
             is_binary(entry_id) and is_binary(id) and is_integer(pr) and pr > 0 and
             is_binary(sha) do
    if state == "QUEUED" and head == nil do
      {:ok, %{pr: pr, node_id: id, entry_id: entry_id, head_sha: sha}}
    else
      {:done, :already_started}
    end
  end

  def queued_tail(_), do: {:error, :incomplete_queue_tail}
end
