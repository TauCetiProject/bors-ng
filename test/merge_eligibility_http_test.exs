defmodule BorsNG.MergeEligibilityHttpTest do
  use ExUnit.Case

  test "real HTTP adapter encodes the check name once and reads every page" do
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

    _server =
      spawn_link(fn ->
        for page <- 1..2 do
          {:ok, socket} = :gen_tcp.accept(listener, 5_000)
          {:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
          send(parent, {:request, request})
          body = Jason.encode!(%{"total_count" => 2, "check_runs" => [%{"id" => page}]})

          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n#{body}"
          )

          :gen_tcp.close(socket)
        end

        send(parent, :server_done)
      end)

    head = String.duplicate("a", 40)

    assert {:ok, [%{"id" => 1}, %{"id" => 2}]} =
             BorsNG.GitHub.Server.do_handle_call(
               :get_eligibility_checks,
               {{:raw, "test-token"}, 14},
               {head}
             )

    for page <- 1..2 do
      assert_receive {:request, request}
      ["GET", path | _] = String.split(request)
      query = path |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()
      assert query["check_name"] == "merge eligibility"
      assert query["page"] == Integer.to_string(page)
      assert query["filter"] == "all"
    end

    assert_receive :server_done
  end
end
