defmodule BorsNG.GitHub.ReadOnlyTest do
  use ExUnit.Case
  alias BorsNG.GitHub
  alias BorsNG.GitHub.{BatchRead, Pr, ReadOnly, Server}
  @conn {{:raw, "test-token"}, 14}
  @head String.duplicate("a", 40)

  defmodule Endpoint do
    def init(handler), do: handler

    def call(conn, handler) do
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      {status, data} = handler.(conn, body)
      Plug.Conn.send_resp(conn, status, Jason.encode!(data))
    end
  end

  defp serve(handler) do
    ref = make_ref()
    {:ok, _} = Plug.Cowboy.http(Endpoint, handler, port: 0, ref: ref)
    port = :ranch.get_port(ref)

    old = for key <- [:api_github_root, :server], do: {key, Application.fetch_env!(:bors, key)}
    Application.put_env(:bors, :api_github_root, "http://127.0.0.1:#{port}")
    Application.put_env(:bors, :server, Server)

    on_exit(fn ->
      Plug.Cowboy.shutdown(ref)
      Enum.each(old, fn {key, value} -> Application.put_env(:bors, key, value) end)
    end)
  end

  defp identity(number) do
    %{
      "number" => number,
      "headRefOid" => @head,
      "baseRefName" => "main",
      "state" => "OPEN",
      "isDraft" => false
    }
  end

  defp rest(number) do
    %{
      "number" => number,
      "title" => "Title",
      "body" => "Body",
      "head" => %{"sha" => @head, "ref" => "update", "repo" => %{"id" => 14}},
      "base" => %{"ref" => "main", "repo" => %{"id" => 14}},
      "user" => %{"id" => 7, "login" => "ada", "avatar_url" => "url"},
      "state" => "open",
      "draft" => false,
      "merged_at" => nil,
      "mergeable" => false,
      "merged" => false
    }
  end

  test "a slow read does not block another read and can finish after eight seconds" do
    parent = self()

    serve(fn conn, _ ->
      if conn.request_path == "/users/slow" do
        send(parent, {:slow_started, self()})
        receive do: (:release -> :ok)
      end

      {404, %{}}
    end)

    slow = Task.async(fn -> GitHub.public_user(@conn, "slow") end)
    assert_receive {:slow_started, handler}, 2000
    assert {:ok, nil} == GitHub.public_user(@conn, "healthy")
    Process.sleep(8100)
    send(handler, :release)
    assert {:ok, nil} == Task.await(slow, 2000)
  end

  test "read HTTP retries recover a transient error" do
    {:ok, count} = Agent.start_link(fn -> 0 end)

    serve(fn _, _ ->
      case Agent.get_and_update(count, &{&1, &1 + 1}) do
        0 -> {503, %{}}
        _ -> {404, %{}}
      end
    end)

    assert {:ok, nil} == GitHub.public_user(@conn, "ada")
    assert Agent.get(count, & &1) == 2
  end

  test "an unauthorized read refreshes its installation token and replays" do
    key = :public_key.generate_key({:rsa, 2048, 65537})
    pem = :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, key)])
    old_config = Application.get_env(:bors, Server)
    Application.put_env(:bors, Server, iss: "test-app", pem: pem)
    state = :sys.get_state(ReadOnly)
    :sys.replace_state(ReadOnly, &%{&1 | tokens: %{91 => {"expired", Joken.current_time()}}})

    on_exit(fn ->
      Application.put_env(:bors, Server, old_config)
      :sys.replace_state(ReadOnly, fn _ -> state end)
    end)

    parent = self()

    serve(fn conn, _ ->
      send(
        parent,
        {:token_path, conn.request_path, Plug.Conn.get_req_header(conn, "authorization")}
      )

      case conn.request_path do
        "/app/installations/91/access_tokens" ->
          {201, %{"token" => "fresh"}}

        "/users/ada" ->
          if Plug.Conn.get_req_header(conn, "authorization") == ["token expired"],
            do: {401, %{}},
            else: {404, %{}}
      end
    end)

    assert {:ok, nil} == GitHub.public_user({{:installation, 91}, 14}, "ada")
    assert_receive {:token_path, "/users/ada", ["token expired"]}
    assert_receive {:token_path, "/app/installations/91/access_tokens", [_jwt]}
    assert_receive {:token_path, "/users/ada", ["token fresh"]}
  end

  test "partial GraphQL identity results retain healthy aliases and recover only missing PRs" do
    parent = self()

    serve(fn conn, _ ->
      send(parent, {:path, conn.request_path})

      case conn.request_path do
        "/graphql" ->
          {200,
           %{
             "data" => %{"repository" => %{"databaseId" => 14, "p1" => identity(1)}},
             "errors" => [%{"path" => ["repository", "p2"]}]
           }}

        "/repositories/14/pulls/2" ->
          {200, rest(2)}
      end
    end)

    assert {:ok, %{1 => %Pr{head_sha: @head}, 2 => %Pr{head_sha: @head}}} =
             BatchRead.fetch(@conn, "owner/repo", [1, 2], :identity)

    assert_receive {:path, "/graphql"}
    assert_receive {:path, "/repositories/14/pulls/2"}
    refute_receive {:path, _}
  end

  test "a failed later chunk preserves earlier metadata and sends only that chunk to REST" do
    {:ok, count} = Agent.start_link(fn -> 0 end)

    serve(fn _, _ ->
      case Agent.get_and_update(count, &{&1, &1 + 1}) do
        0 ->
          # Identity-only nodes deliberately select REST metadata, except the complete first PR.
          raw =
            Map.merge(identity(1), %{
              "title" => "Title",
              "body" => "Body",
              "author" => %{"__typename" => "User", "databaseId" => 7, "login" => "ada"},
              "commits" => %{
                "totalCount" => 1,
                "pageInfo" => %{"hasNextPage" => false},
                "nodes" => [
                  %{
                    "commit" => %{
                      "oid" => @head,
                      "author" => %{"name" => "Ada", "email" => "ada@example.invalid"}
                    }
                  }
                ]
              }
            })

          repo = Map.new(1..20, &{"p#{&1}", if(&1 == 1, do: raw, else: identity(&1))})
          {200, %{"data" => %{"repository" => Map.put(repo, "databaseId", 14)}}}

        _ ->
          {403, %{}}
      end
    end)

    assert {:ok, results} = BatchRead.fetch(@conn, "owner/repo", Enum.to_list(1..41), :metadata)
    assert %{pr: %Pr{number: 1}} = results[1]
    assert Enum.all?(2..41, &(results[&1] == :rest))
    assert Agent.get(count, & &1) == 3
  end

  test "REST recovery validates identity and preserves HTTP errors" do
    serve(fn conn, _ ->
      case conn.request_path do
        "/graphql" -> {403, %{}}
        "/repositories/14/pulls/1" -> {200, put_in(rest(1), ["base", "repo", "id"], 15)}
        "/repositories/14/pulls/2" -> {404, %{}}
      end
    end)

    assert {:error, %{pr: 1, cause: "{:error, :identity_response_invalid}"}} =
             BatchRead.fetch(@conn, "owner/repo", [1], :identity)

    assert {:error, %{pr: 2, cause: "{:error, :get_pr, 404, 2}"}} =
             BatchRead.fetch(@conn, "owner/repo", [2], :identity)
  end

  test "finishing an old read cannot overwrite a concurrent token refresh" do
    parent = self()
    state = :sys.get_state(ReadOnly)
    issued = Joken.current_time()
    :sys.replace_state(ReadOnly, &%{&1 | tokens: %{91 => {"old", issued}}})
    on_exit(fn -> :sys.replace_state(ReadOnly, fn _ -> state end) end)

    serve(fn _, _ ->
      send(parent, {:started, self()})
      receive do: (:release -> :ok)
      {404, %{}}
    end)

    read = Task.async(fn -> GitHub.public_user(@conn, "ada") end)
    assert_receive {:started, handler}, 2000
    :sys.replace_state(ReadOnly, &%{&1 | tokens: %{91 => {"fresh", issued}}})
    send(handler, :release)
    assert {:ok, nil} == Task.await(read)
    assert :sys.get_state(ReadOnly).tokens[91] == {"fresh", issued}
  end

  test "an abandoned caller cancels its HTTP task and releases pending state" do
    parent = self()

    serve(fn _, _ ->
      send(parent, :started)
      receive do: (:release -> :ok)
      {404, %{}}
    end)

    caller = spawn(fn -> GitHub.public_user(@conn, "ada") end)
    assert_receive :started, 2000
    [pid] = Task.Supervisor.children(GitHub.ReadTasks)
    ref = Process.monitor(pid)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2000
    assert :sys.get_state(ReadOnly).pending == %{}
    assert :sys.get_state(ReadOnly).callers == %{}
  end

  test "failed REST commit pagination keeps the HTTP cause" do
    serve(fn _, _ -> {403, %{}} end)
    assert {:error, :get_pr_commits, 403, _url} = GitHub.read_pr_commits(@conn, 1)
  end

  test "a killed read task returns an error and the coordinator stays available" do
    parent = self()

    serve(fn _, _ ->
      send(parent, :request_started)
      receive do: (:release -> :ok)
      {404, %{}}
    end)

    read = Task.async(fn -> GitHub.public_user(@conn, "ada") end)
    assert_receive :request_started, 2000
    [pid] = Task.Supervisor.children(GitHub.ReadTasks)
    Process.exit(pid, :kill)
    assert {:error, :read_worker_unavailable} == Task.await(read)
    assert Process.alive?(Process.whereis(ReadOnly))
  end
end
