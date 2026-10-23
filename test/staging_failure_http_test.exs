defmodule BorsNG.StagingFailureHttpTest do
  use ExUnit.Case
  alias BorsNG.GitHub.Server

  @head String.duplicate("a", 40)
  @base String.duplicate("b", 40)
  @repo "TauCetiProject/TauCeti"

  defp responses(replies, fun) do
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
      for {status, data} <- replies do
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        {:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
        send(parent, {:staging_request, request})
        body = Jason.encode!(data)

        :gen_tcp.send(
          socket,
          "HTTP/1.1 #{status} Result\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n#{body}"
        )

        :gen_tcp.close(socket)
      end

      send(parent, :staging_requests_done)
    end)

    result = fun.()
    assert_receive :staging_requests_done
    result
  end

  defp build(state \\ "failure") do
    [
      %{
        "context" => "build",
        "state" => state,
        "creator" => %{"id" => 41_898_282},
        "target_url" => "https://github.com/#{@repo}/actions/runs/123"
      }
    ]
  end

  defp run do
    %{
      "event" => "repository_dispatch",
      "path" => ".github/workflows/pr-build.yml",
      "display_title" => "bors branch=main batch=7 head=#{@head} base=#{@base}",
      "status" => "completed",
      "conclusion" => "cancelled"
    }
  end

  defp classify do
    Server.do_handle_call(
      :staging_failure,
      {{:raw, "test-token"}, 14},
      {@head, @repo, 7, "main", @base}
    )
  end

  test "recovered finalization reads only the exact run attempt jobs" do
    jobs = [
      %{"name" => "sandboxed-build", "status" => "completed", "conclusion" => "success"},
      %{
        "name" => "publish-merge-group-cache / publish",
        "status" => "completed",
        "conclusion" => "success",
        "steps" => [
          %{
            "name" => "Confirm the exact revision map on the public endpoint",
            "status" => "completed",
            "conclusion" => "success"
          }
        ]
      },
      %{
        "name" => "finalize-merge-group-build",
        "status" => "completed",
        "conclusion" => "failure"
      }
    ]

    fetch = fn ->
      Server.do_handle_call(:recovered_staging_finalization, {{:raw, "test-token"}, 14}, {123, 2})
    end

    assert {:ok, true} = responses([{200, %{"jobs" => jobs, "total_count" => 3}}], fetch)
    assert_receive {:staging_request, request}
    assert request =~ "/repositories/14/actions/runs/123/attempts/2/jobs?per_page=100"
    refute_receive {:staging_request, _}, 10
    assert {:error, :staging_finalization_jobs, 503} = responses([{503, %{}}], fetch)
    assert_receive {:staging_request, _}
  end

  test "classification needs at most one status page and one run lookup" do
    assert {:ok, {:infrastructure, %{conclusion: "cancelled"}}} =
             responses([{200, build()}, {200, run()}], &classify/0)

    assert_receive {:staging_request, first}
    assert_receive {:staging_request, second}
    assert first =~ "/repositories/14/commits/#{@head}/statuses?per_page=100"
    assert second =~ "/repositories/14/actions/runs/123"
    refute_receive {:staging_request, _}, 10
  end

  test "newer pending or successful evidence does not fetch the obsolete run" do
    for state <- ["success", "pending"] do
      assert {:ok, :pending} = responses([{200, build(state)}], &classify/0)
      assert_receive {:staging_request, _}
      refute_receive {:staging_request, _}, 10
    end
  end

  test "read failure and a mismatching run remain inconclusive" do
    assert {:ok, :unavailable} = responses([{503, %{}}], &classify/0)
    assert_receive {:staging_request, _}
    refute_receive {:staging_request, _}, 10

    assert {:ok, :unavailable} =
             responses(
               [{200, build()}, {200, Map.put(run(), "display_title", "another batch")}],
               &classify/0
             )

    assert_receive {:staging_request, _}
    assert_receive {:staging_request, _}
  end

  test "a trusted failed attempt uses one bounded jobs lookup and persists step identity" do
    run = Map.merge(run(), %{"id" => 123, "run_attempt" => 2, "conclusion" => "failure"})

    jobs = %{
      "total_count" => 1,
      "jobs" => [
        %{
          "id" => 456,
          "run_id" => 123,
          "status" => "completed",
          "conclusion" => "failure",
          "steps" => [
            %{
              "number" => 12,
              "name" => "Preflight exact-candidate import headers",
              "conclusion" => "failure"
            }
          ]
        }
      ]
    }

    assert {:ok, {:conclusive, evidence}} =
             responses([{200, build()}, {200, run}, {200, jobs}], &classify/0)

    assert evidence.run_attempt == 2
    assert hd(evidence.failed_steps).job_id == 456
    assert_receive {:staging_request, _}
    assert_receive {:staging_request, _}
    assert_receive {:staging_request, third}
    assert third =~ "/actions/runs/123/attempts/2/jobs?per_page=100"
    refute_receive {:staging_request, _}, 10
  end

  test "exact-base reconsideration rejects truncated and diverged comparisons" do
    call = fn ->
      Server.do_handle_call(:failure_base_changes, {{:raw, "test-token"}, 14}, {@base, @head})
    end

    good = %{
      "status" => "ahead",
      "merge_base_commit" => %{"sha" => @base},
      "files" => [%{"filename" => "new.lean", "previous_filename" => "old.lean"}]
    }

    assert {:ok, ["new.lean", "old.lean"]} = responses([{200, good}], call)
    assert_receive {:staging_request, _}

    for bad <- [
          Map.put(good, "status", "diverged"),
          Map.put(good, "files", List.duplicate(%{"filename" => "f"}, 300)),
          Map.put(good, "merge_base_commit", %{"sha" => @head})
        ] do
      assert {:error, :incomplete_failure_base_changes} = responses([{200, bad}], call)
      assert_receive {:staging_request, _}
    end
  end
end
