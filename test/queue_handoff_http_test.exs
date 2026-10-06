defmodule BorsNG.QueueHandoffHttpTest do
  use ExUnit.Case
  alias BorsNG.GitHub.Server

  @conn {{:raw, "test-token"}, 14}
  @selected_at "2026-10-06T00:08:34Z"

  defp selection(value \\ "bors", at \\ @selected_at) do
    %{
      "total_count" => 1,
      "variables" => [
        %{
          "name" => "MERGE_BACKEND",
          "value" => value,
          "updated_at" => at
        }
      ]
    }
  end

  defp tail(overrides \\ %{}) do
    %{
      "data" => %{
        "repository" => %{
          "mergeQueue" => %{
            "entries" => %{
              "totalCount" => 39,
              "nodes" => [
                Map.merge(
                  %{
                    "id" => "entry-39",
                    "position" => 39,
                    "state" => "QUEUED",
                    "headCommit" => nil,
                    "pullRequest" => %{"id" => "pr-39", "number" => 39, "headRefOid" => "head"}
                  },
                  overrides
                )
              ]
            }
          }
        }
      }
    }
  end

  defp serve(responses) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_, port}} = :inet.sockname(listener)
    old_root = Application.get_env(:bors, :api_github_root)
    old_proxy = System.get_env("HTTPS_PROXY")
    Application.put_env(:bors, :api_github_root, "http://127.0.0.1:#{port}")
    System.delete_env("HTTPS_PROXY")

    on_exit(fn ->
      :gen_tcp.close(listener)
      Application.put_env(:bors, :api_github_root, old_root)
      if old_proxy, do: System.put_env("HTTPS_PROXY", old_proxy)
    end)

    parent = self()

    spawn_link(fn ->
      for response <- responses do
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        request = read_request(socket, "")
        send(parent, {:request, request})
        body = Jason.encode!(response)

        :gen_tcp.send(
          socket,
          "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n#{body}"
        )

        :gen_tcp.close(socket)
      end

      send(parent, :server_done)
    end)
  end

  defp read_request(socket, acc) do
    case String.split(acc, "\r\n\r\n", parts: 2) do
      [headers, body] ->
        [_, length] = Regex.run(~r/content-length:\s*(\d+)/i, headers) || [nil, "0"]
        if byte_size(body) >= String.to_integer(length), do: acc, else: receive_more(socket, acc)

      _ ->
        receive_more(socket, acc)
    end
  end

  defp receive_more(socket, acc) do
    {:ok, data} = :gen_tcp.recv(socket, 0, 5_000)
    read_request(socket, acc <> data)
  end

  test "real adapter reads the live tail and dequeues the PR ID rather than entry ID" do
    serve([
      selection(),
      tail(),
      %{"data" => %{"dequeuePullRequest" => %{"clientMutationId" => "entry-39"}}}
    ])

    assert {:ok, %{pr: 39, head_sha: "head", entry_id: "entry-39"}} =
             Server.do_handle_call(:release_queue_tail, @conn, {@selected_at})

    assert_receive {:request, variables}
    assert variables =~ "GET /repositories/14/actions/variables"
    assert_receive {:request, query}
    assert query =~ "entries(last:1)"
    assert_receive {:request, mutation}
    [_, body] = String.split(mutation, "\r\n\r\n", parts: 2)
    assert Jason.decode!(body)["variables"]["id"] == "pr-39"
    assert Jason.decode!(body)["variables"]["mutationId"] == "entry-39"
    assert Jason.decode!(body)["query"] =~ "clientMutationId"
    assert_receive :server_done
  end

  test "an active tail sends no removal mutation" do
    serve([
      selection(),
      tail(%{"state" => "AWAITING_CHECKS", "headCommit" => %{"oid" => "staging"}})
    ])

    assert {:done, :already_started} =
             Server.do_handle_call(:release_queue_tail, @conn, {@selected_at})

    assert_receive :server_done
  end

  test "a tail that acquired a commit while still QUEUED is retained" do
    serve([selection(), tail(%{"headCommit" => %{"oid" => "staging"}})])

    assert {:done, :already_started} =
             Server.do_handle_call(:release_queue_tail, @conn, {@selected_at})

    assert_receive :server_done
  end

  test "a newer backend selection stops before reading or mutating the queue" do
    serve([selection("bors", "2026-10-06T01:00:00Z")])
    assert {:error, _} = Server.do_handle_call(:release_queue_tail, @conn, {@selected_at})
    assert_receive :server_done
  end

  test "GraphQL partial responses never permit a removal" do
    serve([selection(), Map.put(tail(), "errors", [%{"message" => "unavailable"}])])
    assert {:error, _} = Server.do_handle_call(:release_queue_tail, @conn, {@selected_at})
    assert_receive :server_done
  end

  test "a rejected removal is reported as an error" do
    serve([selection(), tail(), %{"errors" => [%{"message" => "not authorized"}]}])
    assert {:error, _} = Server.do_handle_call(:release_queue_tail, @conn, {@selected_at})
    assert_receive :server_done
  end
end
