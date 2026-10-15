defmodule BorsNG.SourceRequestHttpTest do
  use ExUnit.Case
  @head String.duplicate("a", 40)
  @base String.duplicate("b", 40)

  defp request(author, policy \\ "local-source-v1") do
    %{
      id: 23,
      user: %{login: author},
      body:
        "Merge-queue recovery for head `aaaaaaa`: source failure.\nChecker policy: `#{policy}`.\n<!--tauceti-rebase:v1 #{@head}-->"
    }
  end

  defp serve(responses, action) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_, port}} = :inet.sockname(listener)
    old = Application.get_env(:bors, :api_github_root)
    proxy = System.get_env("HTTPS_PROXY")
    Application.put_env(:bors, :api_github_root, "http://127.0.0.1:#{port}")
    System.delete_env("HTTPS_PROXY")

    on_exit(fn ->
      :gen_tcp.close(listener)
      Application.put_env(:bors, :api_github_root, old)
      if proxy, do: System.put_env("HTTPS_PROXY", proxy)
    end)

    parent = self()

    spawn_link(fn ->
      for {status, response} <- responses do
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        {:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
        send(parent, {:http, request})
        body = Jason.encode!(response)

        :gen_tcp.send(
          socket,
          "HTTP/1.1 #{status} Test\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n#{body}"
        )

        :gen_tcp.close(socket)
      end
    end)

    action.()
  end

  test "resolution edits only the owned local-source marker and retains diagnosis" do
    serve([{200, [request("tau-ceti-bors[bot]")]}, {200, %{}}], fn ->
      assert :ok =
               BorsNG.GitHub.Server.do_handle_call(
                 :resolve_source_request,
                 {{:raw, "test"}, 14},
                 {1, @head, @base}
               )

      assert_receive {:http, get}
      assert get =~ "GET /repositories/14/issues/1/comments?per_page=100"
      assert_receive {:http, patch}
      assert patch =~ "PATCH /repositories/14/issues/comments/23"
      assert patch =~ "Source evidence superseded"
      refute patch =~ "<!--tauceti-rebase:v1"
    end)
  end

  make_request = fn author, policy ->
    %{
      id: 23,
      user: %{login: author},
      body:
        "Merge-queue recovery for head `aaaaaaa`: source failure.\nChecker policy: `#{policy}`.\n<!--tauceti-rebase:v1 #{@head}-->"
    }
  end

  for {name, comments, status, expected} <- [
        {"Review App request", [make_request.("tauceti-review-bot[bot]", "local-source-v1")], 200,
         :other_recovery_request},
        {"authoritative individual failure",
         [make_request.("tau-ceti-bors[bot]", "authoritative-individual-v1")], 200,
         :other_recovery_request},
        {"bounded history", List.duplicate(%{body: "old"}, 100), 200,
         :incomplete_request_history},
        {"API outage", %{}, 503, :incomplete_request_history}
      ] do
    @comments comments
    @status status
    @expected expected
    test "#{name} cannot be silently cleared" do
      serve([{@status, @comments}], fn ->
        assert {:error, @expected} =
                 BorsNG.GitHub.Server.do_handle_call(
                   :resolve_source_request,
                   {{:raw, "test"}, 14},
                   {1, @head, @base}
                 )

        assert_receive {:http, _}
      end)
    end
  end
end
