defmodule BorsNG.OrchestrationHttpTest do
  use ExUnit.Case
  alias BorsNG.GitHub.Server
  @since 1_760_000_000_000_000

  defp response(status, data, fun) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_, port}} = :inet.sockname(listener)
    old = Application.get_env(:bors, :api_github_root)
    Application.put_env(:bors, :api_github_root, "http://127.0.0.1:#{port}")

    on_exit(fn ->
      :gen_tcp.close(listener)
      Application.put_env(:bors, :api_github_root, old)
    end)

    parent = self()

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listener, 5000)
      {:ok, request} = :gen_tcp.recv(socket, 0, 5000)
      send(parent, {:request, request})
      body = Jason.encode!(data)

      :gen_tcp.send(
        socket,
        "HTTP/1.1 #{status} Result\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n#{body}"
      )

      :gen_tcp.close(socket)
    end)

    fun.()
  end

  defp seen do
    Server.do_handle_call(
      :staging_dispatch_seen,
      {{:raw, "test-token"}, 14},
      {"repo", "head", "base", 7, "main", @since}
    )
  end

  test "dispatch reconciliation uses one request-scoped page and exact identity" do
    run = %{
      "event" => "repository_dispatch",
      "display_title" => "bors branch=main batch=7 head=head base=base"
    }

    assert {:ok, true} == response(200, %{"total_count" => 1, "workflow_runs" => [run]}, &seen/0)
    assert_receive {:request, request}
    [_, target | _] = String.split(request, " ")
    query = target |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()
    assert query["per_page"] == "100"

    assert query["created"] ==
             ">=" <>
               (DateTime.from_unix!(@since, :microsecond)
                |> DateTime.add(-60, :second)
                |> DateTime.truncate(:second)
                |> DateTime.to_iso8601())

    refute_receive {:request, _}, 10
  end

  test "an empty complete request window establishes no accepted dispatch" do
    assert {:ok, false} == response(200, %{"total_count" => 0, "workflow_runs" => []}, &seen/0)
    assert_receive {:request, _}
  end

  test "truncated or malformed successful responses never establish absence" do
    assert {:error, :dispatch_lookup_truncated} ==
             response(
               200,
               %{
                 "total_count" => 101,
                 "workflow_runs" => List.duplicate(%{"display_title" => "other"}, 100)
               },
               &seen/0
             )

    assert_receive {:request, _}

    assert {:error, :dispatch_lookup_invalid} ==
             response(200, %{"message" => "incomplete"}, &seen/0)

    assert_receive {:request, _}
  end

  test "rate limited reconciliation makes one call and defers" do
    assert {:error, :dispatch_lookup, 429} ==
             response(429, %{"message" => "rate limited"}, &seen/0)

    assert_receive {:request, _}
    refute_receive {:request, _}, 10
  end

  test "notice replay searches only comments updated in its request window" do
    since = "2026-10-14T04:00:00Z"

    assert {:ok, ["marker"]} ==
             response(200, [%{"body" => "marker"}], fn ->
               Server.do_handle_call(
                 :notice_comments_since,
                 {{:raw, "test-token"}, 14},
                 {1, since}
               )
             end)

    assert_receive {:request, request}
    [_, target | _] = String.split(request, " ")
    query = target |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()
    assert query["since"] == since
    assert query["per_page"] == "100"
  end
end
