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

  test "duplicate exact workflow identities cannot authorize a train verdict" do
    run = %{
      "event" => "repository_dispatch",
      "display_title" => "bors branch=main batch=7 head=head base=base"
    }

    assert {:error, :duplicate_staging_runs} ==
             response(200, %{"total_count" => 2, "workflow_runs" => [run, run]}, &seen/0)

    assert_receive {:request, _}
  end

  test "exact run lookup retains authoritative status and attempt" do
    run = %{
      "id" => 10,
      "run_attempt" => 2,
      "status" => "completed",
      "conclusion" => "success",
      "event" => "repository_dispatch",
      "display_title" => "bors branch=main batch=7 head=head base=base"
    }

    result =
      response(200, %{"total_count" => 1, "workflow_runs" => [run]}, fn ->
        Server.do_handle_call(
          :staging_run,
          {{:raw, "test-token"}, 14},
          {"head", "base", 7, "main", @since}
        )
      end)

    assert result == {:ok, run}
  end

  test "active staging lookup fails closed on incomplete capacity evidence" do
    assert {:error, :active_staging_incomplete} ==
             response(200, %{"total_count" => 100, "workflow_runs" => []}, fn ->
               Server.do_handle_call(:active_staging_count, {{:raw, "test-token"}, 14}, {})
             end)

    assert_receive {:request, request}
    assert request =~ "status=queued"
    refute_receive {:request, _}, 10
  end

  test "cancellation discovery uses the actual dispatch window without candidate head_sha filtering" do
    p = %{
      "head_sha" => "head",
      "base_sha" => "base",
      "batch_id" => 7,
      "branch" => "main",
      "dispatch_at" => @since
    }

    assert {:ok, nil} ==
             response(200, %{"total_count" => 0, "workflow_runs" => []}, fn ->
               Server.do_handle_call(:cancellation_lookup, {{:raw, "test-token"}, 14}, {p})
             end)

    assert_receive {:request, request}
    assert request =~ "event=repository_dispatch"
    refute request =~ "head_sha="
    assert request =~ "per_page=100"
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

  defp advisory(type, args, budget \\ 1_000) do
    old = Application.get_env(:bors, :server)
    Application.put_env(:bors, :server, Server)

    try do
      BorsNG.GitHub.Advisory.handle_call(
        {:advisory, type, {{:raw, "test-token"}, 14}, args,
         System.monotonic_time(:millisecond) + budget},
        {self(), make_ref()},
        %{}
      )
    after
      Application.put_env(:bors, :server, old)
    end
  end

  test "advisory HTTP errors make one attempt without middleware or token refresh retries" do
    assert {:reply, {:error, :post_commit_status, 500, _}, %{}} =
             response(500, %{"message" => "unavailable"}, fn ->
               advisory(:post_commit_status, {"head", :error, "red", "url"})
             end)

    assert_receive {:request, request}
    assert request =~ "/statuses/head"
    refute_receive {:request, _}, 10
    refute Process.get(:github_advisory_deadline)
  end

  test "an advisory request expired behind another API call makes no HTTP request" do
    assert {:reply, {:error, :advisory_request}, %{}} =
             advisory(:post_commit_status, {"head", :ok, "green", "url"}, -1)

    refute Process.get(:github_advisory_deadline)
  end

  test "advisory HTTP service stops at its remaining request budget" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_, port}} = :inet.sockname(listener)
    old = Application.get_env(:bors, :api_github_root)
    Application.put_env(:bors, :api_github_root, "http://127.0.0.1:#{port}")

    on_exit(fn ->
      :gen_tcp.close(listener)
      Application.put_env(:bors, :api_github_root, old)
    end)

    parent = self()

    server =
      spawn(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5000)
        {:ok, request} = :gen_tcp.recv(socket, 0, 5000)
        send(parent, {:request, request})

        receive do
          :stop -> :gen_tcp.close(socket)
        end
      end)

    on_exit(fn -> send(server, :stop) end)
    started = System.monotonic_time(:millisecond)

    assert {:reply, {:error, :advisory_request}, %{}} =
             advisory(:post_commit_status, {"head", :ok, "green", "url"}, 200)

    assert System.monotonic_time(:millisecond) - started < 1_000
    assert_receive {:request, _}
    refute Process.get(:github_advisory_deadline)
  end

  test "advisory HTTP runs while the mutation API client is occupied" do
    :sys.suspend(BorsNG.GitHub)

    try do
      assert {:reply, :ok, %{}} =
               response(201, %{}, fn ->
                 advisory(:post_commit_status, {"head", :ok, "green", "url"})
               end)

      assert_receive {:request, _}
    after
      :sys.resume(BorsNG.GitHub)
    end
  end

  test "an unauthorized advisory evicts its installation token without replay" do
    assert {:reply, {:error, :post_commit_status, 401, _}, %{}} =
             response(401, %{}, fn ->
               Server.handle_call(
                 {:advisory, :post_commit_status, {{:installation, 91}, 14},
                  {"head", :error, "red", "url"}, System.monotonic_time(:millisecond) + 1_000},
                 {self(), make_ref()},
                 %{91 => {"test-token", Joken.current_time()}}
               )
             end)

    assert_receive {:request, _}
    refute_receive {:request, _}, 10
  end

  test "a slowly streaming response cannot extend the advisory operation deadline" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_, port}} = :inet.sockname(listener)
    old = Application.get_env(:bors, :api_github_root)
    Application.put_env(:bors, :api_github_root, "http://127.0.0.1:#{port}")

    on_exit(fn ->
      :gen_tcp.close(listener)
      Application.put_env(:bors, :api_github_root, old)
    end)

    server =
      spawn(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5000)
        {:ok, _} = :gen_tcp.recv(socket, 0, 5000)

        :gen_tcp.send(
          socket,
          "HTTP/1.1 201 Result\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n"
        )

        Enum.each(1..30, fn _ ->
          :gen_tcp.send(socket, " ")
          Process.sleep(30)
        end)

        :gen_tcp.close(socket)
      end)

    on_exit(fn -> Process.exit(server, :kill) end)
    started = System.monotonic_time(:millisecond)

    assert {:reply, {:error, :advisory_request}, %{}} =
             advisory(:post_commit_status, {"head", :ok, "green", "url"}, 200)

    assert System.monotonic_time(:millisecond) - started < 800
  end

  test "bounded post-merge close changes only title and state" do
    assert {:reply, :ok, %{}} =
             response(200, %{}, fn ->
               advisory(:close_merged_pr, {1, "[Merged by Bors] - Title"})
             end)

    assert_receive {:request, request}
    assert request =~ "PATCH /repositories/14/pulls/1 "
    [_headers, body] = String.split(request, "\r\n\r\n", parts: 2)
    assert Jason.decode!(body) == %{"state" => "closed", "title" => "[Merged by Bors] - Title"}
  end

  test "expired shared cleanup deadline cannot start a late close" do
    deadline = System.monotonic_time(:millisecond) - 1
    conn = {{:raw, "test-token"}, 14}
    assert {:error, :cleanup_deadline} == BorsNG.GitHub.get_pr_bounded(conn, 1, deadline)

    assert {:error, :cleanup_deadline} ==
             BorsNG.GitHub.close_pr_bounded(conn, 1, "Title", deadline)
  end

  test "batch reads use one GraphQL request and validate repository identity" do
    head = String.duplicate("a", 40)

    data = %{
      "data" => %{
        "repository" => %{
          "databaseId" => 14,
          "p1" => %{
            "number" => 1,
            "headRefOid" => head,
            "baseRefName" => "main",
            "state" => "OPEN",
            "isDraft" => false
          }
        }
      }
    }

    assert {:reply, {:ok, %{1 => %BorsNG.GitHub.Pr{head_sha: ^head}}}, %{}} =
             response(200, data, fn ->
               Server.handle_call(
                 {:batch_read, {{:raw, "test-token"}, 14},
                  {"TauCetiProject/TauCeti", [1], :identity}},
                 nil,
                 %{}
               )
             end)

    assert_receive {:request, request}
    assert request =~ "POST /graphql "
    [_headers, body] = String.split(request, "\r\n\r\n", parts: 2)
    assert Jason.decode!(body)["variables"] == %{"owner" => "TauCetiProject", "name" => "TauCeti"}
    refute_receive {:request, _}, 10
  end

  test "REST attribution preserves all three commit pages in chronological order" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_, port}} = :inet.sockname(listener)
    old = Application.get_env(:bors, :api_github_root)
    root = "http://127.0.0.1:#{port}"
    Application.put_env(:bors, :api_github_root, root)

    on_exit(fn ->
      :gen_tcp.close(listener)
      Application.put_env(:bors, :api_github_root, old)
    end)

    spawn_link(fn ->
      for page <- 1..3 do
        {:ok, socket} = :gen_tcp.accept(listener, 5000)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 5000)

        numbers =
          case page do
            1 -> 1..30
            2 -> 31..60
            3 -> 61..65
          end

        body =
          Jason.encode!(
            Enum.map(numbers, fn number ->
              %{
                "sha" => "head#{number}",
                "commit" => %{
                  "author" => %{
                    "name" => "Author#{number}",
                    "email" => "a#{number}@example.invalid"
                  },
                  "message" => "Message",
                  "tree" => %{"sha" => "tree"}
                }
              }
            end)
          )

        link =
          if page < 3,
            do:
              "Link: <#{root}/repositories/14/pulls/1/commits?page=#{page + 1}>; rel=\"next\"\r\n",
            else: ""

        :gen_tcp.send(
          socket,
          "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n#{link}Content-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n#{body}"
        )

        :gen_tcp.close(socket)
      end
    end)

    assert {:ok, commits} =
             Server.do_handle_call(:get_pr_commits, {{:raw, "test-token"}, 14}, {1})

    assert Enum.map(commits, & &1.sha) == Enum.map(1..65, &"head#{&1}")
    assert Enum.map(commits, & &1.author_name) == Enum.map(1..65, &"Author#{&1}")
  end
end
