require Logger

defmodule BorsNG.GitHub.Server do
  use GenServer
  alias BorsNG.GitHub

  @moduledoc """
  Provides a real connection to GitHub's REST API.
  This doesn't currently do rate limiting, but it will.
  """

  def start_link do
    GenServer.start_link(__MODULE__, :ok, name: GitHub)
  end

  @installation_content_type "application/vnd.github.machine-man-preview+json"
  @team_content_type "application/vnd.github.hellcat-preview+json"
  @content_type_raw "application/vnd.github.v3.raw"
  @content_type "application/vnd.github.v3+json"

  # Page size for `pulls/{n}/files`. GitHub caps this endpoint at 100 per page
  # and 3000 files total; see DELEGATION_INVALIDATION.md, "GitHub API file
  # ceilings".
  @pr_files_per_page 100

  @type tconn :: GitHub.tconn()
  @type ttoken :: GitHub.ttoken()
  @type trepo :: GitHub.trepo()
  @type tuser :: GitHub.tuser()
  @type tpr :: GitHub.tpr()
  @type tcollaborator :: GitHub.tcollaborator()
  @type tuser_repo_perms :: GitHub.tuser_repo_perms()

  @typedoc """
  The token cache.
  """
  @type ttokenreg :: %{number => {binary, number}}

  @spec config() :: keyword
  defp config do
    Confex.fetch_env!(:bors, GitHub.Server)
  end

  @spec site() :: bitstring
  defp site do
    Confex.fetch_env!(:bors, :api_github_root)
  end

  def init(:ok) do
    {:ok, %{}}
  end

  def handle_call({:advisory, type, {token, repo_xref}, args, deadline}, _from, state)
      when type in [
             :post_comment,
             :notice_comments_since,
             :post_commit_status,
             :get_pr,
             :close_merged_pr,
             :cleanup_policy,
             :batch_read,
             :public_user,
             :cancellation_lookup,
             :cancel_candidate_run
           ] do
    # Include time spent behind another API request. Never execute an expired
    # advisory call or hide retries/token refreshes inside its service budget.
    Process.put(:github_advisory_deadline, deadline)

    try do
      if deadline <= System.monotonic_time(:millisecond), do: throw(:advisory_expired)
      {raw, state} = raw_token!(token, state)
      result = do_handle_call(type, {raw, repo_xref}, args)

      state =
        case {token, result} do
          {{:installation, id}, {:error, _, 401, _}} -> Map.delete(state, id)
          {{:installation, id}, {:error, _, 401}} -> Map.delete(state, id)
          _ -> state
        end

      {:reply, result, state}
    rescue
      _ -> {:reply, {:error, :advisory_request}, state}
    catch
      _, _ -> {:reply, {:error, :advisory_request}, state}
    after
      Process.delete(:github_advisory_deadline)
    end
  end

  def handle_call({type, {{_, _} = token, repo_xref}, args}, _from, state) do
    use_token!(token, state, fn token ->
      do_handle_call(type, {token, repo_xref}, args)
    end)
  end

  def handle_call({type, {_, _} = token, args}, _from, state) do
    use_token!(token, state, fn token ->
      do_handle_call(type, token, args)
    end)
  end

  def handle_call(:get_app, _from, state) do
    result =
      "Bearer #{get_jwt_token()}"
      |> tesla_client(@installation_content_type)
      |> Tesla.get!("/app")
      |> case do
        %{body: raw, status: 200} ->
          app_link =
            raw
            |> Jason.decode!()
            |> Map.get("html_url")

          {:ok, app_link}

        %{body: body, status: status} ->
          {:error, :get_app, status, body}
      end

    {:reply, result, state}
  end

  def handle_call(:get_installation_list, _from, state) do
    jwt_token = get_jwt_token()

    list =
      get_installation_list_!(
        jwt_token,
        "#{site()}/app/installations",
        []
      )

    {:reply, {:ok, list}, state}
  end

  def do_handle_call(:git_token, {{:raw, token}, _}, {}), do: {:ok, token}

  def do_handle_call(:get_commit_tree, conn, {sha}) do
    case get!(conn, "git/commits/#{sha}") do
      %{status: 200, body: body} -> {:ok, Jason.decode!(body)["tree"]["sha"]}
      _ -> {:error, :commit_tree_unavailable}
    end
  rescue
    _ -> {:error, :commit_tree_unavailable}
  end

  def do_handle_call(:local_git_mode, conn, {}) do
    case get!(conn, "actions/variables/BORS_CANDIDATE_ASSEMBLER") do
      %{status: 200, body: body} -> {:ok, Jason.decode!(body)["value"]}
      %{status: 404} -> {:ok, "off"}
      _ -> {:error, :local_git_setting_unavailable}
    end
  rescue
    _ -> {:error, :local_git_setting_unavailable}
  end

  def do_handle_call(:get_pr_files, {{:raw, token}, repo_xref}, {pr_xref}) do
    get_pr_files_(
      token,
      "#{site()}/repositories/#{repo_xref}/pulls/#{pr_xref}/files?per_page=#{@pr_files_per_page}",
      []
    )
  end

  def do_handle_call(:get_pr_compare, repo_conn, {base, head}) do
    case get!(repo_conn, "compare/#{base}...#{head}") do
      %{body: raw, status: 200} ->
        files =
          raw
          |> Jason.decode!()
          |> Map.get("files", [])
          |> Enum.map(&GitHub.File.from_json!/1)

        {:ok, files}

      e ->
        {:error, :get_pr_compare, e.status, {base, head}}
    end
  end

  def do_handle_call(:failure_base_changes, repo_conn, {base, head}) do
    with %{status: 200, body: raw} <- get!(repo_conn, "compare/#{base}...#{head}"),
         {:ok, data} <- Jason.decode(raw),
         "ahead" <- data["status"],
         ^base <- get_in(data, ["merge_base_commit", "sha"]),
         files when is_list(files) and length(files) < 300 <- data["files"] do
      {:ok,
       Enum.flat_map(files, fn file ->
         Enum.reject([file["filename"], file["previous_filename"]], &is_nil/1)
       end)}
    else
      _ -> {:error, :incomplete_failure_base_changes}
    end
  end

  def do_handle_call(:compare_status, repo_conn, {base, head}) do
    case get!(repo_conn, "compare/#{base}...#{head}") do
      %{body: raw, status: 200} ->
        case raw |> Jason.decode!() |> Map.get("status") do
          "ahead" -> {:ok, :ahead}
          "behind" -> {:ok, :behind}
          "identical" -> {:ok, :identical}
          "diverged" -> {:ok, :diverged}
          _ -> {:error, :compare_status}
        end

      e ->
        {:error, :compare_status, e.status, {base, head}}
    end
  end

  def do_handle_call(:get_pr, repo_conn, {pr_xref}) do
    case get!(repo_conn, "pulls/#{pr_xref}") do
      %{body: raw, status: 200} ->
        pr =
          raw
          |> Jason.decode!()
          |> GitHub.Pr.from_json!()

        {:ok, pr}

      e ->
        {:error, :get_pr, e.status, pr_xref}
    end
  end

  def do_handle_call(:update_pr_base, repo_conn, pr) do
    repo_conn
    |> patch!(
      "pulls/#{pr.number}",
      Jason.encode!(%{
        title: pr.title,
        body: pr.body,
        state: pr.state,
        base: pr.base_ref
      })
    )
    |> case do
      %{body: raw, status: 200} ->
        pr =
          raw
          |> Jason.decode!()
          |> GitHub.Pr.from_json!()

        {:ok, pr}

      %{body: body, status: status} ->
        {:error, :push, status, body}
    end
  end

  def do_handle_call(:public_user, {token, _repo_id}, {login}),
    do: do_handle_call(:get_user_by_login, token, {login})

  def do_handle_call(:batch_read, {{:raw, token}, repo_id}, {repository, numbers, mode}) do
    request = GitHub.BatchRead.request(repository, numbers, mode)

    case Tesla.post(tesla_client("token #{token}"), "/graphql", Jason.encode!(request)) do
      {:ok, %{status: 200, body: body}} ->
        GitHub.BatchRead.decode(Jason.decode!(body), repo_id, numbers, mode)

      {:ok, %{status: status}} ->
        {:error, :batch_read, status}

      _ ->
        {:error, :batch_read_unavailable}
    end
  end

  def do_handle_call(:cleanup_policy, conn, {commit}) do
    case do_handle_call(:get_file, conn, {commit, "bors.toml"}) do
      {:ok, nil} -> cleanup_policy_file(conn, commit, ".github/bors.toml")
      {:ok, content} -> {:ok, content}
      {:error, _, status, _, _} -> {:error, :cleanup_policy, status}
      _ -> {:error, :cleanup_policy}
    end
  end

  def do_handle_call(:close_merged_pr, conn, {number, title}) do
    # Update only bookkeeping fields; never replay a previously fetched body.
    case patch!(conn, "pulls/#{number}", Jason.encode!(%{state: "closed", title: title})) do
      %{status: 200} -> :ok
      %{status: status} -> {:error, :close_merged_pr, status}
    end
  end

  def do_handle_call(:update_pr, repo_conn, pr) do
    repo_conn
    |> patch!(
      "pulls/#{pr.number}",
      Jason.encode!(%{
        title: pr.title,
        body: pr.body,
        state: pr.state
      })
    )
    |> case do
      %{body: raw, status: 200} ->
        pr =
          raw
          |> Jason.decode!()
          |> GitHub.Pr.from_json!()

        {:ok, pr}

      %{body: body, status: status} ->
        {:error, :push, status, body}
    end
  end

  def do_handle_call(:get_pr_commits, {{:raw, token}, repo_xref}, {pr_xref}) do
    get_pr_commits_(token, "#{site()}/repositories/#{repo_xref}/pulls/#{pr_xref}/commits", [])
  end

  def do_handle_call(:get_open_prs, {{:raw, token}, repo_xref}, {}) do
    {:ok,
     get_open_prs_!(
       token,
       "#{site()}/repositories/#{repo_xref}/pulls?state=open",
       []
     )}
  end

  def do_handle_call(:get_open_prs_with_base, {{:raw, token}, repo_xref}, {base}) do
    {:ok,
     get_open_prs_!(
       token,
       "#{site()}/repositories/#{repo_xref}/pulls?state=open&base=#{base}",
       []
     )}
  end

  def do_handle_call(:push, repo_conn, {sha, to}) do
    repo_conn
    |> patch!("git/refs/heads/#{to}", Jason.encode!(%{sha: sha}))
    |> case do
      %{body: _, status: 200} ->
        {:ok, sha}

      %{body: body, status: status} ->
        IO.inspect({:error, :push, body})
        {:error, :push, status, body}
    end
  end

  def do_handle_call(
        :dispatch_staging,
        {{:raw, token}, _repo_xref},
        {repo_name, head_sha, base_sha, batch_id, members, base_ref}
      ) do
    body =
      Jason.encode!(%{
        event_type: "tauceti-bors-staging",
        client_payload: %{
          head_sha: head_sha,
          base_sha: base_sha,
          batch_id: batch_id,
          members: members,
          base_ref: base_ref
        }
      })

    "token #{token}"
    |> tesla_client(@content_type)
    |> Tesla.post!("/repos/#{repo_name}/dispatches", body)
    |> case do
      %{status: 204} -> :ok
      %{status: status, body: response} -> {:error, :dispatch_staging, status, response}
    end
  end

  def do_handle_call(:cancellation_lookup, conn, {p}) do
    do_handle_call(
      :staging_run,
      conn,
      {p["head_sha"], p["base_sha"], p["batch_id"], p["branch"], p["dispatch_at"]}
    )
  end

  def do_handle_call(:cancel_candidate_run, conn, {id, run_id, attempt}) do
    alias BorsNG.Worker.CandidateCancellation

    # Token acquisition and mailbox wait may outlive the original decision.
    # Re-read the bound run and durable withdrawal before normal cancellation.
    with {:ok, %{"run_id" => ^run_id, "run_attempt" => ^attempt} = authority} <-
           CandidateCancellation.authority(id),
         %{status: 200, body: raw} <- get!(conn, "actions/runs/#{run_id}"),
         {:ok, run} <- Jason.decode(raw),
         true <- is_map(run) and run["id"] == run_id,
         {:ok, _} <- CandidateCancellation.validate_run(run, authority),
         {:ok, ^authority} <- CandidateCancellation.authority(id) do
      case post!(conn, "actions/runs/#{run_id}/cancel", "") do
        %{status: 202} -> :ok
        %{status: 409} -> {:terminal, :already_completed}
        response -> cancellation_error(response)
      end
    else
      {:terminal, _} = terminal -> terminal
      %{status: _} = response -> cancellation_error(response)
      {:error, reason} = error when reason in [:invalid_run_binding, :invalid_run_state] -> error
      {:error, %Jason.DecodeError{}} -> {:error, :cancel_candidate_response, 502}
      false -> {:error, :invalid_run_binding}
      _ -> {:error, :unauthorized_or_expired}
    end
  end

  def do_handle_call(:active_staging_count, conn, {}) do
    Enum.reduce_while(
      ["queued", "in_progress", "waiting", "requested", "pending"],
      {:ok, MapSet.new()},
      fn status, {:ok, ids} ->
        path =
          "actions/workflows/pr-build.yml/runs?event=repository_dispatch&status=#{status}&per_page=100"

        case get!(conn, path) do
          %{status: 200, body: raw} ->
            body = Jason.decode!(raw)
            runs = body["workflow_runs"]

            if is_list(runs) and length(runs) < 100 and body["total_count"] == length(runs) and
                 Enum.all?(runs, &(is_integer(&1["id"]) and is_binary(&1["display_title"]))) do
              staged =
                Enum.filter(runs, &String.starts_with?(&1["display_title"], "bors branch="))

              {:cont, {:ok, Enum.reduce(staged, ids, &MapSet.put(&2, &1["id"]))}}
            else
              {:halt, {:error, :active_staging_incomplete}}
            end

          _ ->
            {:halt, {:error, :active_staging_unavailable}}
        end
      end
    )
    |> case do
      {:ok, ids} -> {:ok, MapSet.size(ids)}
      error -> error
    end
  end

  def do_handle_call(:staging_dispatch_seen, conn, {_repo, head, base, batch, branch, since}) do
    case do_handle_call(:staging_run, conn, {head, base, batch, branch, since}) do
      {:ok, nil} -> {:ok, false}
      {:ok, _run} -> {:ok, true}
      error -> error
    end
  end

  def do_handle_call(:staging_run, conn, {head, base, batch, branch, since}) do
    expected = "bors branch=#{branch} batch=#{batch} head=#{head} base=#{base}"

    created =
      since
      |> DateTime.from_unix!(:microsecond)
      |> DateTime.add(-60, :second)
      |> DateTime.truncate(:second)
      |> DateTime.to_iso8601()

    path =
      "actions/workflows/pr-build.yml/runs?event=repository_dispatch&per_page=100&created=" <>
        ">=" <> created

    case get!(conn, path) do
      %{status: 200, body: raw} ->
        body = Jason.decode!(raw)
        runs = body["workflow_runs"]

        cond do
          not is_list(runs) or not is_integer(body["total_count"]) or body["total_count"] < 0 ->
            {:error, :dispatch_lookup_invalid}

          length(runs) < 100 and body["total_count"] == length(runs) ->
            matches =
              Enum.filter(
                runs,
                &(&1["display_title"] == expected and &1["event"] == "repository_dispatch")
              )

            case matches do
              [] -> {:ok, nil}
              [run] -> {:ok, run}
              _ -> {:error, :duplicate_staging_runs}
            end

          true ->
            {:error, :dispatch_lookup_truncated}
        end

      %{status: status} ->
        {:error, :dispatch_lookup, status}
    end
  end

  def do_handle_call(:recovered_staging_finalization, conn, {run_id, attempt})
      when is_integer(run_id) and run_id > 0 and is_integer(attempt) and attempt > 0 do
    path = "actions/runs/#{run_id}/attempts/#{attempt}/jobs?per_page=100"

    case get!(conn, path) do
      %{status: 200, body: raw} ->
        case Jason.decode(raw) do
          {:ok, jobs} -> {:ok, BorsNG.Worker.StagingFailure.recovered_finalization?(jobs)}
          _ -> {:error, :staging_finalization_jobs}
        end

      %{status: status} ->
        {:error, :staging_finalization_jobs, status}
    end
  end

  def do_handle_call(:merge_backend_snapshot, {{:raw, token}, _} = conn, {}) do
    {:ok, setting} = backend_selection(conn)
    backend = setting.backend

    query =
      ~s|{repository(owner:#{Jason.encode!(hd(String.split(BorsNG.ProjectConfig.repo(), "/")))},name:#{Jason.encode!(List.last(String.split(BorsNG.ProjectConfig.repo(), "/")))}){mergeQueue(branch:#{Jason.encode!(BorsNG.ProjectConfig.branch())}){entries(first:1){totalCount}}}}|

    response =
      "token #{token}"
      |> tesla_client(@content_type)
      |> Tesla.post!("/graphql", Jason.encode!(%{query: query}))

    data = Jason.decode!(response.body)
    count = get_in(data, ["data", "repository", "mergeQueue", "entries", "totalCount"])

    if response.status == 200 and not Map.has_key?(data, "errors") and
         backend in ["queue", "bors"] and is_integer(count) and count >= 0 do
      {:ok, Map.put(setting, :github_count, count)}
    else
      {:error, :invalid_backend_observation}
    end
  rescue
    _ -> {:error, :backend_observation_failed}
  end

  def do_handle_call(:release_queue_tail, {{:raw, token}, _} = conn, {selected_at, deadline}) do
    # Check the absolute deadline inside the server too: a timed-out caller's
    # request can remain in this GenServer's mailbox and execute later.
    with true <- System.monotonic_time(:millisecond) < deadline,
         {:ok, %{backend: "bors", updated_at: ^selected_at}} <- backend_selection(conn),
         %{status: 200, body: raw} <-
           "token #{token}"
           |> tesla_client(@content_type)
           |> Tesla.post!(
             "/graphql",
             Jason.encode!(%{
               query:
                 ~s|{repository(owner:#{Jason.encode!(hd(String.split(BorsNG.ProjectConfig.repo(), "/")))},name:#{Jason.encode!(List.last(String.split(BorsNG.ProjectConfig.repo(), "/")))}){mergeQueue(branch:#{Jason.encode!(BorsNG.ProjectConfig.branch())}){entries(last:1){totalCount nodes{id position state headCommit{oid} pullRequest{id number headRefOid}}}}}}|
             })
           ),
         data <- Jason.decode!(raw),
         false <- Map.has_key?(data, "errors"),
         {:ok, entry} <-
           BorsNG.Worker.QueueHandoff.queued_tail(
             get_in(data, ["data", "repository", "mergeQueue", "entries"])
           ),
         true <- System.monotonic_time(:millisecond) < deadline do
      dequeue_tail(token, entry, selected_at)
    else
      {:done, reason} -> {:done, reason}
      _ -> {:error, :queue_handoff_deferred}
    end
  rescue
    _ -> {:error, :queue_handoff_unavailable}
  end

  def do_handle_call(:merge_candidates, {{:raw, token}, _}, {}) do
    {:ok, merge_candidates(token, nil, [], MapSet.new())}
  rescue
    _ -> {:error, :merge_candidates_unavailable}
  end

  def do_handle_call(:merge_candidates_page, {{:raw, token}, _}, {cursor}) do
    {:ok, merge_candidates_page(token, cursor)}
  rescue
    _ -> {:error, :merge_candidates_unavailable}
  end

  def do_handle_call(:get_eligibility_checks, conn, {head}) do
    {:ok, eligibility_checks(conn, head, 1, [])}
  rescue
    _ -> {:error, :eligibility_checks_unavailable}
  end

  def do_handle_call(:get_pr_merge_base, conn, {number, head}) do
    branch = BorsNG.ProjectConfig.branch()

    with %{status: 200, body: raw} <- get!(conn, "pulls/#{number}"),
         %{
           "state" => "open",
           "base" => %{"ref" => ^branch, "sha" => base},
           "head" => %{"sha" => ^head}
         } <- Jason.decode!(raw),
         %{status: 200, body: raw} <- get!(conn, "compare/#{base}...#{head}?per_page=1"),
         %{"merge_base_commit" => %{"sha" => merge_base}} <- Jason.decode!(raw) do
      {:ok, merge_base}
    else
      _ -> {:error, :merge_base_unavailable}
    end
  rescue
    _ -> {:error, :merge_base_unavailable}
  end

  def do_handle_call(:get_staging_base, conn, {head}) do
    with true <- Regex.match?(~r/\A[0-9a-f]{40}\z/, head),
         %{status: 200, body: raw} <- get!(conn, "git/commits/#{head}"),
         %{"sha" => ^head, "parents" => [%{"sha" => base} | _]} <- Jason.decode!(raw),
         true <- Regex.match?(~r/\A[0-9a-f]{40}\z/, base) do
      {:ok, base}
    else
      _ -> {:error, :staging_base_unavailable}
    end
  rescue
    _ -> {:error, :staging_base_unavailable}
  end

  def do_handle_call(:post_bump_failure, conn, {head, batch}) do
    body =
      Jason.encode!(%{
        context: "bump-integration",
        state: "failure",
        description: "Confirmed singleton integration failure; bump repair required",
        target_url:
          "https://#{System.get_env("PUBLIC_HOST", "bors.taucetiproject.org")}/batches/#{batch}"
      })

    case post!(conn, "statuses/#{head}", body) do
      %{status: 201} -> :ok
      _ -> {:error, :bump_failure_status}
    end
  rescue
    _ -> {:error, :bump_failure_status}
  end

  def do_handle_call(:reset_scope, conn, {head}) do
    body =
      Jason.encode!(%{
        context: "scope",
        state: "pending",
        description: "Human scope approval changed; waiting for trusted CI"
      })

    case post!(conn, "statuses/#{head}", body) do
      %{status: 201} -> :ok
      _ -> {:error, :scope_reset_failed}
    end
  rescue
    _ -> {:error, :scope_reset_failed}
  end

  def do_handle_call(:dispatch_pr_build, conn, {pr}) do
    body = Jason.encode!(%{event_type: "tauceti-bors-pr-approved", client_payload: %{pr: pr}})

    case post!(conn, "dispatches", body) do
      %{status: 204} -> :ok
      _ -> {:error, :pr_build_dispatch_failed}
    end
  rescue
    _ -> {:error, :pr_build_dispatch_failed}
  end

  def do_handle_call(:dispatch_reconcile, conn, {}) do
    case post!(conn, "dispatches", Jason.encode!(%{event_type: "tauceti-merge-reconcile"})) do
      %{status: 204} -> :ok
      _ -> {:error, :reconcile_dispatch_failed}
    end
  rescue
    _ -> {:error, :reconcile_dispatch_failed}
  end

  def do_handle_call(:get_branch, repo_conn, {branch}) do
    case get!(repo_conn, "branches/#{branch}") do
      %{body: raw, status: 200} ->
        r = Jason.decode!(raw)["commit"]
        {:ok, %{commit: r["sha"], tree: r["commit"]["tree"]["sha"]}}

      %{body: body, status: status} ->
        {:error, :get_branch, status, body}
    end
  end

  def do_handle_call(:delete_branch, repo_conn, {branch}) do
    case delete!(repo_conn, "git/refs/heads/#{branch}") do
      %{status: 204} ->
        :ok

      _ ->
        {:error, :delete_branch}
    end
  end

  def do_handle_call(
        :merge_branch,
        repo_conn,
        {%{
           from: from,
           to: to,
           commit_message: commit_message
         }}
      ) do
    msg = %{base: to, head: from, commit_message: commit_message}

    repo_conn
    |> post!("merges", Jason.encode!(msg))
    |> case do
      %{body: raw, status: 201} ->
        data = Jason.decode!(raw)

        res = %{
          commit: data["sha"],
          tree: data["commit"]["tree"]["sha"]
        }

        {:ok, res}

      %{status: 409} ->
        {:ok, :conflict}

      %{status: 204} ->
        {:ok, :conflict}

      %{body: body, status: status, headers: headers} ->
        {:error, :merge_branch, status, body, Map.new(headers)["x-github-request-id"]}
    end
  end

  def do_handle_call(
        :create_commit,
        repo_conn,
        {%{
           tree: tree,
           parents: parents,
           commit_message: commit_message,
           committer: committer
         }}
      ) do
    msg = %{parents: parents, tree: tree, message: commit_message}

    msg =
      if is_nil(committer) do
        msg
      else
        Map.put(msg, "author", %{
          name: committer.name,
          email: committer.email
        })
      end

    resp =
      repo_conn
      |> post!("git/commits", Jason.encode!(msg))
      |> case do
        %{body: raw, status: 201} ->
          Logger.info("Raw response from GH #{inspect(raw)}")
          data = Jason.decode!(raw)

          res = %{
            commit: data["sha"]
          }

          {:ok, res.commit}

        %{status: 409} ->
          {:ok, :conflict}

        %{status: 204} ->
          {:ok, :conflict}

        %{body: body, status: status, headers: headers} ->
          {:error, :create_commit, status, body, Map.new(headers)["x-github-request-id"]}
      end

    resp
  end

  def do_handle_call(
        :synthesize_commit,
        repo_conn,
        {%{
           branch: branch,
           tree: tree,
           parents: parents,
           commit_message: commit_message,
           committer: committer
         }}
      ) do
    msg = %{parents: parents, tree: tree, message: commit_message}

    msg =
      if is_nil(committer) do
        msg
      else
        Map.put(msg, "author", %{
          name: committer.name,
          email: committer.email
        })
      end

    repo_conn
    |> post!("git/commits", Jason.encode!(msg))
    |> case do
      %{body: raw, status: 201} ->
        sha = Jason.decode!(raw)["sha"]
        do_handle_call(:force_push, repo_conn, {sha, branch})

      %{body: body, status: status, headers: headers} ->
        {:error, :synthesize_commit, status, body, Map.new(headers)["x-github-request-id"]}
    end
  end

  def do_handle_call(:force_push, repo_conn, {sha, to}) do
    repo_conn
    |> get!("branches/#{to}")
    |> case do
      %{status: 404} ->
        msg = %{ref: "refs/heads/#{to}", sha: sha}

        repo_conn
        |> post!("git/refs", Jason.encode!(msg))
        |> case do
          %{status: 201} ->
            {:ok, sha}

          %{status: status, body: body} ->
            {:error, :force_push, status, body}
        end

      %{body: raw, status: 200} ->
        if sha != Jason.decode!(raw)["commit"]["sha"] do
          msg = %{force: true, sha: sha}

          repo_conn
          |> patch!("git/refs/heads/#{to}", Jason.encode!(msg))
          |> case do
            %{status: 200} ->
              {:ok, sha}

            %{status: status, body: body} ->
              {:error, :force_push, status, body}
          end
        else
          {:ok, sha}
        end

      %{body: body, status: status, headers: headers} ->
        {:error, :force_push, status, body, Map.new(headers)["x-github-request-id"]}
    end
  end

  def do_handle_call(:get_commit_status, {{:raw, token}, repo_xref}, {sha}) do
    with {:ok, status} <-
           get_statuses_!(
             token,
             "#{site()}/repositories/#{repo_xref}/commits/#{sha}/status",
             %{}
           ),
         {:ok, check} <-
           get_checks_!(
             token,
             "#{site()}/repositories/#{repo_xref}/commits/#{sha}/check-runs",
             %{}
           ),
         do: {:ok, Map.merge(status, check)}
  end

  def do_handle_call(:staging_failure, repo_conn, {head, repo, batch, branch, base}) do
    alias BorsNG.Worker.StagingFailure

    # The combined /status response omits creator identity. The exact-head
    # status list includes it and is returned newest first. Limit this
    # failure-only lookup to one page; incomplete evidence remains a hold.
    with %{status: 200, body: raw} <- get!(repo_conn, "commits/#{head}/statuses?per_page=100"),
         {:ok, statuses} when is_list(statuses) <- Jason.decode(raw) do
      case StagingFailure.build_source(%{"sha" => head, "statuses" => statuses}, head, repo) do
        {:lookup, id} ->
          with %{status: 200, body: raw} <- get!(repo_conn, "actions/runs/#{id}"),
               {:ok, run} <- Jason.decode(raw) do
            case StagingFailure.classify_run(run, batch, head, branch, base) do
              :conclusive ->
                attempt = run["run_attempt"]

                with true <- is_integer(attempt) and attempt > 0,
                     %{status: 200, body: jobs_raw} <-
                       get!(repo_conn, "actions/runs/#{id}/attempts/#{attempt}/jobs?per_page=100"),
                     {:ok, jobs} <- Jason.decode(jobs_raw) do
                  {:ok, StagingFailure.classify_jobs(run, jobs)}
                else
                  _ -> {:ok, :unavailable}
                end

              :cancelled ->
                {:ok, {:infrastructure, StagingFailure.run_evidence(run)}}

              result ->
                {:ok, result}
            end
          else
            _ -> {:ok, :unavailable}
          end

        result ->
          {:ok, result}
      end
    else
      _ -> {:ok, :unavailable}
    end
  rescue
    _ -> {:ok, :unavailable}
  end

  def do_handle_call(:get_labels, repo_conn, {issue_xref}) do
    repo_conn
    |> get!("issues/#{issue_xref}/labels")
    |> case do
      %{body: raw, status: 200} ->
        res =
          Jason.decode!(raw)
          |> Enum.map(fn %{"name" => name} -> name end)

        {:ok, res}

      %{body: body, status: status} ->
        {:error, :get_labels, status, body}
    end
  end

  def do_handle_call(:add_labels, repo_conn, {issue_xref, labels}) do
    repo_conn
    |> post!("issues/#{issue_xref}/labels", Jason.encode!(%{labels: labels}))
    |> case do
      %{status: status} when status in [200, 201] ->
        :ok

      %{body: body, status: status} ->
        {:error, :add_labels, status, body}
    end
  end

  def do_handle_call(:remove_label, {{:raw, token}, repo_xref}, {issue_xref, label}) do
    # The label is a single path segment, so it must be www-form-encoded: a label
    # may legitimately contain `/`, `?`, or `#`, none of which `URI.encode`
    # escapes (they are reserved characters). We can't route this through the
    # shared `delete!` either — it runs the whole path through `URI.encode`, which
    # would double-encode the `%` in our escapes — so we build the path directly,
    # the way `:get_user_by_login` does. A 404 means the label was already absent,
    # which is success for an idempotent remove.
    "token #{token}"
    |> tesla_client()
    |> Tesla.delete!(
      "/repositories/#{repo_xref}/issues/#{issue_xref}/labels/#{URI.encode_www_form(label)}"
    )
    |> case do
      %{status: status} when status in [200, 404] ->
        :ok

      %{body: body, status: status} ->
        {:error, :remove_label, status, body}
    end
  end

  def do_handle_call(:list_issues_by_label, {{:raw, token}, repo_xref}, {label}) do
    {:ok,
     get_issues_by_label_!(
       token,
       "#{site()}/repositories/#{repo_xref}/issues?state=open&per_page=100&labels=" <>
         URI.encode_www_form(label),
       []
     )}
  end

  def do_handle_call(:get_reviews, {{:raw, token}, repo_xref}, {issue_xref, sha}) do
    reviews =
      token
      |> get_reviews_json_!("#{site()}/repositories/#{repo_xref}/pulls/#{issue_xref}/reviews", [])
      |> GitHub.Reviews.filter_sha!(sha)
      |> GitHub.Reviews.from_json!()

    {:ok, reviews}
  end

  def do_handle_call(:get_reviews, repo_conn, {issue_xref}) do
    do_handle_call(:get_reviews, repo_conn, {issue_xref, nil})
  end

  def do_handle_call(:get_repo_tree, repo_conn, {branch}) do
    with {:branch, %{body: branch_raw, status: 200}} <-
           {:branch, get!(repo_conn, "branches/#{branch}")},
         tree_sha <- Jason.decode!(branch_raw)["commit"]["commit"]["tree"]["sha"],
         {:tree, %{body: tree_raw, status: 200}} <-
           {:tree, get!(repo_conn, "git/trees/#{tree_sha}?recursive=1")},
         decoded <- Jason.decode!(tree_raw),
         # GitHub caps recursive trees (~100k entries / 7MB). A truncated tree
         # is missing paths, so reporting it as complete would make the lint
         # emit false "matches no files" warnings. Truncation is deterministic
         # (a refetch truncates too), so return it as a terminal {:ok, ...} so
         # call_with_retry stops immediately rather than burning the retry
         # budget re-fetching a multi-MB tree that can never come back whole.
         # Callers must skip on :truncated rather than treat it as complete.
         {:truncated, false} <- {:truncated, decoded["truncated"] == true} do
      paths =
        decoded
        |> Map.get("tree", [])
        |> Enum.filter(&(&1["type"] == "blob"))
        |> Enum.map(& &1["path"])

      {:ok, paths}
    else
      {:branch, %{status: status, body: body}} -> {:error, :get_repo_tree, status, body}
      {:tree, %{status: status, body: body}} -> {:error, :get_repo_tree, status, body}
      {:truncated, true} -> {:ok, {:truncated, branch}}
    end
  end

  def do_handle_call(:get_file, repo_conn, {branch, path}) do
    resp =
      get!(
        repo_conn,
        "contents/#{path}",
        @content_type_raw,
        query: [ref: branch]
      )

    case resp do
      %{body: raw, status: 200} ->
        {:ok, raw}

      %{status: 404} ->
        {:ok, nil}

      %{body: body, status: status, headers: headers} ->
        {:error, :get_file, status, body, Map.new(headers)["x-github-request-id"]}

      %{body: body, status: status} ->
        {:error, :get_file, status, body, nil}
    end
  end

  def do_handle_call(:post_comment, repo_conn, {number, body}) do
    repo_conn
    |> post!("issues/#{number}/comments", Jason.encode!(%{body: body}))
    |> case do
      %{status: 201} ->
        :ok

      %{status: status, body: raw} ->
        {:error, :post_comment, status, raw}
    end
  end

  def do_handle_call(:resolve_source_request, conn, {pr, head, base}) do
    marker = "<!--tauceti-rebase:v1 #{head}-->"
    # Bound history coverage. An incomplete read cannot cancel someone else's
    # request or authorize staging; ordinary reconciliation can retry it.
    with %{status: 200, body: raw} <- get!(conn, "issues/#{pr}/comments?per_page=100"),
         comments when is_list(comments) and length(comments) < 100 <- Jason.decode!(raw) do
      requests =
        Enum.filter(comments, fn c ->
          body = c["body"] || ""

          get_in(c, ["user", "login"]) in ["tau-ceti-bors[bot]", "tauceti-review-bot[bot]"] and
            String.starts_with?(body, "Merge-queue recovery for head `") and
            String.contains?(body, marker)
        end)

      own =
        Enum.filter(
          requests,
          &(get_in(&1, ["user", "login"]) == "tau-ceti-bors[bot]" and
              Enum.any?(~w(local-source-v2 local-source-v3), fn policy ->
                String.contains?(&1["body"], "Checker policy: `#{policy}`")
              end))
        )

      if length(own) == length(requests) and length(own) <= 4 do
        Enum.reduce_while(own, :ok, fn comment, _ ->
          body =
            String.replace(
              comment["body"],
              marker,
              "Source evidence superseded: exact local-source check passes against main `#{base}`. No branch was mutated."
            )

          case patch!(conn, "issues/comments/#{comment["id"]}", Jason.encode!(%{body: body})) do
            %{status: 200} -> {:cont, :ok}
            _ -> {:halt, {:error, :comment_resolution_failed}}
          end
        end)
      else
        {:error, :other_recovery_request}
      end
    else
      _ -> {:error, :incomplete_request_history}
    end
  end

  def do_handle_call(:get_pr_comments, repo_conn, {number}) do
    case get!(repo_conn, "issues/#{number}/comments?per_page=100") do
      %{body: raw, status: 200} ->
        bodies =
          raw
          |> Jason.decode!()
          |> Enum.map(&Map.get(&1, "body"))
          |> Enum.reject(&is_nil/1)

        {:ok, bodies}

      e ->
        {:error, :get_pr_comments, e.status, number}
    end
  end

  def do_handle_call(:notice_comments_since, conn, {number, since}) do
    path = "issues/#{number}/comments?per_page=100&since=" <> since

    case get!(conn, path) do
      %{status: 200, body: raw} ->
        comments = Jason.decode!(raw)

        if is_list(comments),
          do: {:ok, Enum.map(comments, & &1["body"])},
          else: {:error, :comment_lookup_invalid}

      %{status: status} ->
        {:error, :comment_lookup, status}
    end
  end

  def do_handle_call(:post_commit_status, repo_conn, {sha, status, msg, url}) do
    state = GitHub.map_status_to_state(status)
    body = %{state: state, context: "bors", description: msg, target_url: url}

    repo_conn
    |> post!("statuses/#{sha}", Jason.encode!(body))
    |> case do
      %{status: 201} ->
        :ok

      %{status: status, body: raw} ->
        {:error, :post_commit_status, status, raw}
    end
  end

  def do_handle_call(:belongs_to_team, repo_conn, {org, team_slug, username}) do
    IO.inspect(repo_conn)
    {{:raw, token}, _installation_id} = repo_conn

    "token #{token}"
    |> tesla_client()
    |> Tesla.get!(URI.encode("/orgs/#{org}/teams/#{team_slug}/memberships/#{username}"))
    |> case do
      %{status: 200} ->
        true

      %{status: 404} ->
        false

      _ ->
        false
    end
  end

  def do_handle_call(:get_collaborators_by_repo, {{:raw, token}, repo_xref}, {}) do
    get_collaborators_by_repo_(
      token,
      "#{site()}/repositories/#{repo_xref}/collaborators",
      []
    )
  end

  def do_handle_call(:repository_admin, {{:raw, token}, repo_xref}, {actor}) do
    client = tesla_client("token #{token}")

    case Tesla.get(client, "/user/#{actor}") do
      {:ok, %{status: 404}} ->
        {:ok, false}

      {:ok, %{status: 200, body: user_body}} ->
        repository_admin_permission(client, repo_xref, actor, user_body)

      _ ->
        {:error, :repository_admin_unavailable}
    end
  end

  def do_handle_call(
        :get_user_by_login,
        {:raw, token},
        {login}
      ) do
    "token #{token}"
    |> tesla_client()
    |> Tesla.get!("/users/#{URI.encode_www_form(login)}")
    |> case do
      %{body: raw, status: 200} ->
        user =
          raw
          |> Jason.decode!()
          |> GitHub.FullUser.from_json!()

        {:ok, user}

      %{status: 404} ->
        {:ok, nil}

      _ ->
        {:error, :get_user_by_login}
    end
  end

  def do_handle_call(:get_installation_repos, {:raw, token}, {}) do
    {:ok,
     get_installation_repos_!(
       token,
       "#{site()}/installation/repositories",
       []
     )}
  end

  @spec get_statuses_!(binary, binary | nil, %{bitstring => GitHub.tstatus()}) ::
          {:ok, %{bitstring => GitHub.tstatus()}}
  defp get_statuses_!(_, nil, statuses) do
    statuses
  end

  defp get_statuses_!(token, url, statuses) do
    params = get_url_params(url)

    {raw, headers} =
      "token #{token}"
      |> tesla_client(@content_type)
      |> Tesla.get!(url, query: params)
      |> case do
        %{body: raw, status: 200, headers: headers} -> {raw, headers}
        _ -> {~s({"statuses":[]}), %{}}
      end

    statuses =
      Jason.decode!(raw)["statuses"]
      |> Enum.map(
        &{
          &1["context"] |> GitHub.map_changed_status(),
          GitHub.map_state_to_status(&1["state"])
        }
      )
      |> Map.new()
      |> Map.merge(statuses)

    next_headers = get_next_headers(headers)

    case next_headers do
      [] -> {:ok, statuses}
      [next] -> get_statuses_!(token, next.url, statuses)
    end
  end

  @spec get_checks_!(binary, binary | nil, %{bitstring => GitHub.tstatus()}) ::
          {:ok, %{bitstring => GitHub.tstatus()}}
  defp get_checks_!(_, nil, checks) do
    checks
  end

  defp get_checks_!(token, url, checks) do
    params = get_url_params(url)

    {raw, headers} =
      "token #{token}"
      |> tesla_client(@content_type)
      |> Tesla.get!(url, query: params)
      |> case do
        %{body: raw, status: 200, headers: headers} -> {raw, headers}
        _ -> {~s({"check_runs":[]}), %{}}
      end

    checks =
      Jason.decode!(raw)["check_runs"]
      |> Enum.map(
        &{
          &1["name"] |> GitHub.map_changed_status(),
          GitHub.map_check_to_status(&1["conclusion"])
        }
      )
      |> Map.new()
      |> Map.merge(checks)

    next_headers = get_next_headers(headers)

    case next_headers do
      [] -> {:ok, checks}
      [next] -> get_checks_!(token, next.url, checks)
    end
  end

  defp get_reviews_json_!(_, nil, append) do
    append
  end

  defp get_reviews_json_!(token, url, append) do
    params = get_url_params(url)

    %{body: raw, status: 200, headers: headers} =
      "token #{token}"
      |> tesla_client(@installation_content_type)
      |> Tesla.get!(url, query: params)

    json = Enum.concat(append, Jason.decode!(raw))
    next_headers = get_next_headers(headers)

    case next_headers do
      [] -> json
      [next] -> get_reviews_json_!(token, next.url, json)
    end
  end

  @spec get_installation_repos_!(binary, binary, [trepo]) :: [trepo]
  defp get_installation_repos_!(_, nil, repos) do
    repos
  end

  defp get_installation_repos_!(token, url, append) do
    params = get_url_params(url)

    %{body: raw, status: 200, headers: headers} =
      "token #{token}"
      |> tesla_client(@installation_content_type)
      |> Tesla.get!(url, query: params)

    repositories =
      Jason.decode!(raw)["repositories"]
      |> Enum.map(&GitHub.Repo.from_json!/1)
      |> Enum.concat(append)

    next_headers = get_next_headers(headers)

    case next_headers do
      [] -> repositories
      [next] -> get_installation_repos_!(token, next.url, repositories)
    end
  end

  @spec get_installation_list_!(binary, binary | nil, [integer]) :: [integer]
  defp get_installation_list_!(_, nil, list) do
    list
  end

  defp get_installation_list_!(jwt_token, url, append) do
    params = get_url_params(url)

    %{body: raw, status: 200, headers: headers} =
      "Bearer #{jwt_token}"
      |> tesla_client(@installation_content_type)
      |> Tesla.get!(url, query: params)

    list =
      Jason.decode!(raw)
      |> Enum.map(fn %{"id" => id} -> id end)
      |> Enum.concat(append)

    next_headers = get_next_headers(headers)

    case next_headers do
      [] -> list
      [next] -> get_installation_list_!(jwt_token, next.url, list)
    end
  end

  @spec get_open_prs_!(binary, binary | nil, [tpr]) :: [tpr]
  defp get_open_prs_!(_, nil, prs) do
    prs
  end

  defp get_open_prs_!(token, url, append) do
    params = get_url_params(url)

    {raw, headers} =
      "token #{token}"
      |> tesla_client(@content_type)
      |> Tesla.get!(url, query: params)
      |> case do
        %{body: raw, status: 200, headers: headers} -> {raw, headers}
        _ -> {"[]", %{}}
      end

    prs =
      Jason.decode!(raw)
      |> Enum.flat_map(fn %{"url" => url} ->
        "token #{token}"
        |> tesla_client(@content_type)
        |> Tesla.get!(url)
        |> case do
          %{body: raw, status: 200} -> Jason.decode!(raw)
          _ -> %{}
        end
        |> GitHub.Pr.from_json()
        |> case do
          {:ok, pr} -> [pr]
          _ -> []
        end
      end)
      |> Enum.concat(append)

    next_headers = get_next_headers(headers)

    case next_headers do
      [] -> prs
      [next] -> get_open_prs_!(token, next.url, prs)
    end
  end

  # Paginate the repo's issues list filtered by label. Returns `{number,
  # [label_name]}` per issue — the issues endpoint already embeds each item's
  # full label set, so the backstop sweep gets every PR's current labels in the
  # same response it uses to discover them, with no extra per-PR read. The
  # `?labels=` filter is applied server-side, so a repo with thousands of open
  # PRs still only returns the (few) PRs carrying the label.
  @spec get_issues_by_label_!(binary, binary | nil, [{integer, [binary]}]) ::
          [{integer, [binary]}]
  defp get_issues_by_label_!(_, nil, acc) do
    acc
  end

  defp get_issues_by_label_!(token, url, acc) do
    params = get_url_params(url)

    {raw, headers} =
      "token #{token}"
      |> tesla_client(@content_type)
      |> Tesla.get!(url, query: params)
      |> case do
        %{body: raw, status: 200, headers: headers} -> {raw, headers}
        _ -> {"[]", %{}}
      end

    acc =
      raw
      |> Jason.decode!()
      |> Enum.map(fn issue ->
        names = Enum.map(issue["labels"] || [], fn %{"name" => name} -> name end)
        {issue["number"], names}
      end)
      |> Enum.concat(acc)

    case get_next_headers(headers) do
      [] -> acc
      [next] -> get_issues_by_label_!(token, next.url, acc)
    end
  end

  # Paginate `pulls/{n}/files`. GitHub enforces the 3000-file cap by returning
  # empty pages past it while the Link header keeps advertising more, so we stop
  # on the first short page (fewer than per_page) rather than when rel="next"
  # disappears — otherwise a multi-thousand-file PR fetches dozens of empty
  # pages. See DELEGATION_INVALIDATION.md, "GitHub API file ceilings".
  defp get_pr_files_(token, url, acc) do
    "token #{token}"
    |> tesla_client(@content_type)
    |> Tesla.get(url, query: get_url_params(url))
    |> case do
      {:ok, %{body: raw, status: 200, headers: headers}} ->
        files = raw |> Jason.decode!() |> Enum.map(&GitHub.File.from_json!/1)
        acc = acc ++ files

        if length(files) < @pr_files_per_page do
          {:ok, acc}
        else
          case get_next_headers(headers) do
            [] -> {:ok, acc}
            [next] -> get_pr_files_(token, next.url, acc)
          end
        end

      {:ok, %{status: status}} ->
        {:error, :get_pr_files, status, url}

      _ ->
        {:error, :get_pr_files}
    end
  end

  defp get_pr_commits_(_, nil, append) do
    {:ok, append}
  end

  defp get_pr_commits_(token, url, append) do
    params = get_url_params(url)

    "token #{token}"
    |> tesla_client(@team_content_type)
    |> Tesla.get(url, query: params)
    |> case do
      {:ok, %{body: raw, status: 200, headers: headers}} ->
        Logger.info("Raw response from GH #{inspect(raw)}")

        commits =
          raw
          |> Jason.decode!()
          |> Enum.map(&GitHub.Commit.from_json!/1)

        next_headers = get_next_headers(headers)

        case next_headers do
          [] -> {:ok, append ++ commits}
          [next] -> get_pr_commits_(token, next.url, append ++ commits)
        end

      {:error, %{status: status}} ->
        {:error, :get_pr_commits, status, url}

      error ->
        IO.inspect(error)
        {:error, :get_pr_commits}
    end
  end

  defp repository_admin_permission(client, repo_xref, actor, user_body) do
    with {:ok, %{"id" => ^actor, "login" => login}} <- Jason.decode(user_body),
         true <- is_binary(login),
         {:ok, %{status: 200, body: permission_body}} <-
           Tesla.get(
             client,
             "/repositories/#{repo_xref}/collaborators/#{URI.encode_www_form(login)}/permission"
           ),
         {:ok, %{"user" => %{"id" => ^actor}, "permission" => permission}} <-
           Jason.decode(permission_body) do
      {:ok, permission == "admin"}
    else
      _ -> {:error, :repository_admin_unavailable}
    end
  end

  @spec extract_user_repo_perms(map()) :: tuser_repo_perms
  defp extract_user_repo_perms(data) do
    Map.new(BorsNG.Database.ProjectPermission.string_list_no_nil(), fn perm ->
      {String.to_atom(perm), !!data["permissions"][perm]}
    end)
  end

  @spec get_collaborators_by_repo_(binary, binary, [tcollaborator]) ::
          {:ok, [tcollaborator]} | {:error, :get_collaborators_by_repo}
  def get_collaborators_by_repo_(token, url, append) do
    params = get_url_params(url)

    "token #{token}"
    |> tesla_client(@team_content_type)
    |> Tesla.get(url, query: params)
    |> case do
      {:ok, %{body: raw, status: 200, headers: headers}} ->
        users =
          raw
          |> Jason.decode!()
          |> Enum.map(fn user ->
            %{user: GitHub.User.from_json!(user), perms: extract_user_repo_perms(user)}
          end)
          |> Enum.concat(append)

        next_headers = get_next_headers(headers)

        case next_headers do
          [] ->
            {:ok, users}

          [next] ->
            get_collaborators_by_repo_(token, next.url, users)
        end

      error ->
        IO.inspect(error)
        {:error, :get_collaborators_by_repo}
    end
  end

  defp eligibility_checks(conn, head, page, acc) do
    path =
      "commits/#{head}/check-runs?check_name=merge eligibility&filter=all&per_page=100&page=#{page}"

    %{status: 200, body: raw} = get!(conn, path)
    %{"check_runs" => checks, "total_count" => total} = Jason.decode!(raw)
    true = is_list(checks) and is_integer(total) and total >= 0
    acc = acc ++ checks

    if length(acc) >= total,
      do: acc,
      else:
        (
          true = checks != []
          eligibility_checks(conn, head, page + 1, acc)
        )
  end

  defp merge_candidates(token, cursor, acc, seen) do
    page = merge_candidates_page(token, cursor)
    all = acc ++ page.candidates

    if page.next_cursor do
      next = page.next_cursor
      if MapSet.member?(seen, next), do: raise("invalid pagination")
      merge_candidates(token, next, all, MapSet.put(seen, next))
    else
      all
    end
  end

  defp merge_candidates_page(token, cursor) do
    query =
      ~s|query($cursor:String){repository(owner:#{Jason.encode!(hd(String.split(BorsNG.ProjectConfig.repo(), "/")))},name:#{Jason.encode!(List.last(String.split(BorsNG.ProjectConfig.repo(), "/")))}){pullRequests(first:100,after:$cursor,states:OPEN,baseRefName:#{Jason.encode!(BorsNG.ProjectConfig.branch())}){pageInfo{hasNextPage endCursor}nodes{number headRefOid isDraft isInMergeQueue labels(first:100){nodes{name}pageInfo{hasNextPage}}}}}}|

    response =
      "token #{token}"
      |> tesla_client(@content_type)
      |> Tesla.post!("/graphql", Jason.encode!(%{query: query, variables: %{cursor: cursor}}))

    data = Jason.decode!(response.body)
    if response.status != 200 or data["errors"], do: raise("candidate query failed")
    connection = get_in(data, ["data", "repository", "pullRequests"])

    nodes =
      Enum.map(connection["nodes"], fn p ->
        if get_in(p, ["labels", "pageInfo", "hasNextPage"]), do: raise("labels truncated")
        labels = MapSet.new(p["labels"]["nodes"], & &1["name"])

        ready =
          not p["isDraft"] and MapSet.member?(labels, "ready-to-merge") and
            MapSet.disjoint?(labels, MapSet.new(["keep", "hold", "wip", "human", "do-not-close"]))

        %{pr: p["number"], head_sha: p["headRefOid"], queued: p["isInMergeQueue"], ready: ready}
      end)

    page = connection["pageInfo"]
    next = if page["hasNextPage"], do: page["endCursor"], else: nil
    if page["hasNextPage"] and (is_nil(next) or next == cursor), do: raise("invalid pagination")
    %{candidates: nodes, next_cursor: next}
  end

  defp dequeue_tail(token, entry, selected_at) do
    response =
      "token #{token}"
      |> tesla_client(@content_type)
      |> Tesla.post!(
        "/graphql",
        Jason.encode!(%{
          query:
            "mutation($id:ID!,$mutationId:String!){dequeuePullRequest(input:{id:$id,clientMutationId:$mutationId}){clientMutationId}}",
          variables: %{id: entry.node_id, mutationId: entry.entry_id}
        })
      )

    result = Jason.decode!(response.body)

    if response.status == 200 and not Map.has_key?(result, "errors") and
         get_in(result, ["data", "dequeuePullRequest", "clientMutationId"]) == entry.entry_id do
      # Log in the mutation owner even if its caller has already timed out.
      log_queue_handoff(entry, selected_at, "released")
      {:ok, Map.drop(entry, [:node_id])}
    else
      log_queue_handoff(entry, selected_at, "ambiguous")
      {:error, :queue_handoff_ambiguous}
    end
  rescue
    _ ->
      log_queue_handoff(entry, selected_at, "ambiguous")
      {:error, :queue_handoff_ambiguous}
  end

  defp log_queue_handoff(entry, selected_at, outcome) do
    Logger.log(
      if(outcome == "ambiguous", do: :warning, else: :info),
      Jason.encode!(%{
        schema: "tauceti-merge.handoff/v1",
        from: "queue",
        to: "bors",
        selected_at: selected_at,
        outcome: outcome,
        entry: Map.drop(entry, [:node_id])
      })
    )
  end

  defp backend_selection(conn) do
    variables = backend_variables(conn, 1, [])
    found = Enum.filter(variables, &(&1["name"] == "MERGE_BACKEND"))

    case found do
      [] ->
        {:ok, %{backend: "queue", updated_at: nil}}

      [%{"value" => value, "updated_at" => updated_at}] when value in ["queue", "bors"] ->
        {:ok, %{backend: value, updated_at: updated_at}}

      _ ->
        {:error, :invalid_backend_selection}
    end
  end

  defp backend_variables(conn, page, acc) do
    response = get!(conn, "actions/variables?per_page=100&page=#{page}")
    data = Jason.decode!(response.body)

    if response.status != 200 or not is_list(data["variables"]) or
         not is_integer(data["total_count"]),
       do: raise("variables unavailable")

    if data["variables"] == [] and length(acc) < data["total_count"],
      do: raise("variables truncated")

    if Enum.any?(data["variables"], &(not is_binary(&1["name"]) or not is_binary(&1["value"]))),
      do: raise("variables malformed")

    all = acc ++ data["variables"]
    if length(all) >= data["total_count"], do: all, else: backend_variables(conn, page + 1, all)
  end

  @spec post!(tconn, binary, binary, binary) :: map
  defp post!(
         {{:raw, token}, repo_xref},
         path,
         body,
         content_type \\ @content_type
       ) do
    "token #{token}"
    |> tesla_client(content_type)
    |> Tesla.post!(URI.encode("/repositories/#{repo_xref}/#{path}"), body)
  end

  @spec patch!(tconn, binary, binary, binary) :: map
  defp patch!(
         {{:raw, token}, repo_xref},
         path,
         body,
         content_type \\ @content_type
       ) do
    "token #{token}"
    |> tesla_client(content_type)
    |> Tesla.patch!(URI.encode("/repositories/#{repo_xref}/#{path}"), body)
  end

  @spec get!(tconn, binary, binary, list) :: map
  defp get!(
         {{:raw, token}, repo_xref},
         path,
         content_type \\ @content_type,
         params \\ []
       ) do
    "token #{token}"
    |> tesla_client(content_type)
    |> Tesla.get!(URI.encode("/repositories/#{repo_xref}/#{path}"), params)
  end

  @spec delete!(tconn, binary, binary, list) :: map
  defp delete!(
         {{:raw, token}, repo_xref},
         path,
         content_type \\ @content_type,
         params \\ []
       ) do
    "token #{token}"
    |> tesla_client(content_type)
    |> Tesla.delete!(URI.encode("/repositories/#{repo_xref}/#{path}"), params)
  end

  defp get_next_headers(headers) do
    Enum.flat_map(headers, fn {name, value} ->
      name
      |> String.downcase(:ascii)
      |> case do
        "link" ->
          case Regex.run(~r/<([^>]+)>;\s*rel="next"/, value) do
            [_, url] -> [%{url: url}]
            nil -> []
          end

        _ ->
          []
      end
    end)
  end

  defp get_url_params(url) do
    case URI.parse(url).query do
      nil -> []
      qry -> URI.query_decoder(qry) |> Enum.to_list()
    end
  end

  @token_exp 60

  @spec get_installation_token!(number) :: binary
  def get_installation_token!(installation_xref) do
    jwt_token = get_jwt_token()

    %{body: raw, status: 201} =
      "Bearer #{jwt_token}"
      |> tesla_client(@installation_content_type)
      |> Tesla.post!("app/installations/#{installation_xref}/access_tokens", "")

    Jason.decode!(raw)["token"]
  end

  def get_jwt_token do
    import Joken.Config
    cfg = config()

    Joken.generate_and_sign!(
      default_claims(),
      %{
        "iat" => Joken.current_time(),
        "exp" => Joken.current_time() + @token_exp,
        "iss" => cfg[:iss]
      },
      Joken.Signer.create("RS256", %{"pem" => cfg[:pem]})
    )
  end

  @doc """
  Uses a token from the cache, or, if the request fails,
  retry without using the cached token.
  """
  @spec use_token!(ttoken, ttokenreg, (ttoken -> term)) ::
          {:reply, term, ttokenreg}
  def use_token!({:installation, installation_xref} = token, state, fun) do
    {token, state} = raw_token!(token, state)
    result = fun.(token)

    case result do
      {:ok, _} ->
        {:reply, result, state}

      :ok ->
        {:reply, result, state}

      _ ->
        state = Map.delete(state, installation_xref)
        {token, state} = raw_token!(token, state)
        result = fun.(token)
        {:reply, result, state}
    end
  end

  def use_token!(token, state, fun) do
    {token, state} = raw_token!(token, state)
    result = fun.(token)
    {:reply, result, state}
  end

  @doc """
  Given an {:installation, installation_xref},
  look it up in the token cache.
  If it's there, and it's still usable, use it.
  Otherwise, fetch a new one.
  """
  @spec raw_token!(ttoken, ttokenreg) :: {{:raw, binary}, ttokenreg}
  def raw_token!({:installation, installation_xref}, state) do
    now = Joken.current_time()

    case state[installation_xref] do
      {token, issued} when issued + @token_exp > now ->
        {{:raw, token}, state}

      _ ->
        token = get_installation_token!(installation_xref)
        state = Map.put(state, installation_xref, {token, now})
        {{:raw, token}, state}
    end
  end

  def raw_token!({:raw, _} = raw, state) do
    {raw, state}
  end

  defp tesla_client(authorization, content_type \\ @content_type) do
    deadline = Process.get(:github_advisory_deadline)
    remaining = if deadline, do: deadline - System.monotonic_time(:millisecond)
    if deadline && remaining <= 1, do: throw(:advisory_expired)

    middleware = [
      {Tesla.Middleware.BaseUrl, site()},
      {Tesla.Middleware.Headers,
       [
         {"authorization", authorization},
         {"accept", content_type},
         {"user-agent", "bors-ng https://bors.tech"}
       ]}
    ]

    middleware =
      if deadline,
        do: middleware,
        else: middleware ++ [{Tesla.Middleware.Retry, delay: 100, max_retries: 5}]

    middleware =
      if Confex.get_env(:bors, :log_outgoing, false) do
        middleware ++ [{Tesla.Middleware.Logger, filter_headers: ["authorization"], debug: true}]
      else
        middleware
      end

    connect_timeout =
      if deadline,
        do: max(remaining, 1),
        else: Confex.get_env(:bors, :api_github_timeout, 8_000) - 1

    recv_timeout =
      if deadline,
        do: max(remaining, 1),
        else: Confex.get_env(:bors, :api_github_timeout, 8_000) - 1

    params =
      [
        connect_timeout: connect_timeout,
        recv_timeout: recv_timeout,
        ssl_options: BorsNG.TLS.hackney_ssl_options()
      ] ++
        case System.get_env("HTTPS_PROXY") do
          nil ->
            []

          proxy ->
            [proxy: proxy]
        end

    Tesla.client(
      middleware,
      {Tesla.Adapter.Hackney, params}
    )
  end

  defp cleanup_policy_file(conn, commit, path) do
    case do_handle_call(:get_file, conn, {commit, path}) do
      {:ok, content} -> {:ok, content}
      {:error, _, status, _, _} -> {:error, :cleanup_policy, status}
      _ -> {:error, :cleanup_policy}
    end
  end

  defp cancellation_error(%{status: 403, headers: headers}) do
    limited =
      Enum.any?(headers, fn {name, value} ->
        String.downcase(name) == "retry-after" or
          (String.downcase(name) == "x-ratelimit-remaining" and value == "0")
      end)

    if limited,
      do: {:error, :cancel_candidate_rate_limit, 429},
      else: {:error, :cancel_candidate_run, 403}
  end

  defp cancellation_error(%{status: code}), do: {:error, :cancel_candidate_run, code}
end
