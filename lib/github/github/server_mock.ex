defmodule BorsNG.GitHub.ServerMock do
  use GenServer

  @moduledoc """
  Provides a fake connection to GitHub's REST API.

  This is used for unit testing and when running in a "dev" environment,
  like on a local machine.
  It's basically just a genserver frontend for a big map;
  you can put and get its state,
  and other functions will mutate or read subsets of it.

  For example, I can run `iex -S mix phx.server` and do this:

      iex> # Push state to "GitHub"
      iex> alias BorsNG.GitHub
      iex> alias BorsNG.GitHub.ServerMock
      iex> ServerMock.put_state(%{
      ...>   {:installation, 91} => %{ repos: [
      ...>     %GitHub.Repo{
      ...>       id: 14,
      ...>       name: "test/repo",
      ...>       owner: %{
      ...>         id: 6,
      ...>         login: "bors-fanboi",
      ...>         avatar_url: "data:image/svg+xml,<svg></svg>",
      ...>         type: :user
      ...>       }}
      ...>   ] },
      ...>   {{:installation, 91}, 14} => %{
      ...>     branches: %{},
      ...>     commits: %{},
      ...>     comments: %{1 => []},
      ...>     pulls: %{
      ...>       1 => %GitHub.Pr{
      ...>         number: 1,
      ...>         title: "Test",
      ...>         body: "Mess",
      ...>         state: :open,
      ...>         base_ref: "master",
      ...>         head_sha: "00000001",
      ...>         user: %GitHub.User{
      ...>           id: 6,
      ...>           login: "bors-fanboi",
      ...>           avatar_url: "data:image/svg+xml,<svg></svg>"}}},
      ...>     pr_commits: %{
      ...>       1 => [
      ...>         %GitHub.Commit{
      ...>           sha: "00000001",
      ...>           author_name: "Bors Fanboi",
      ...>           author_email: "bors-fanboi@example.com"}
      ...>       ] },
      ...>     statuses: %{},
      ...>     files: %{}}})
      iex> GitHub.get_open_prs!({{:installation, 91}, 14})
      [
        %BorsNG.GitHub.Pr{
          number: 1,
          title: "Test",
          body: "Mess",
          state: :open,
          base_ref: "master",
          head_sha: "00000001",
          user: %BorsNG.GitHub.User{
            id: 6,
            login: "bors-fanboi",
            avatar_url: "data:image/svg+xml,<svg></svg>"}}]
  """

  def start_link do
    GenServer.start_link(__MODULE__, :ok, name: BorsNG.GitHub)
  end

  @type tconn :: BorsNG.GitHub.tconn()
  @type ttoken :: BorsNG.GitHub.ttoken()
  @type trepo :: BorsNG.GitHub.trepo()
  @type tuser :: BorsNG.GitHub.User.t()
  @type tcollaborator :: BorsNG.GitHub.tcollaborator()

  @type tbranch :: bitstring
  @type tcommit :: bitstring

  @type tsynthesized :: %{
          commit_message: bitstring,
          parents: [bitstring]
        }

  @type tstate :: %{
          tconn => %{
            branches: %{tbranch => tcommit},
            comments: %{integer => [bitstring]},
            commits: %{bitstring => tsynthesized},
            statuses: %{tbranch => %{bitstring => :open | :closed | :running}},
            files: %{tbranch => %{bitstring => bitstring}},
            collaborators: [tcollaborator]
          },
          {:installation, number} => %{
            repos: [trepo]
          },
          :users => %{bitstring => tuser},
          :merge_conflict => integer,
          :create_commit_error => integer,
          :get_pr_error => integer,
          :get_pr_files_error => integer,
          :get_commit_status_error => integer,
          :get_labels_error => integer,
          :list_issues_by_label_error => boolean,
          :get_reviews_error => integer,
          :post_commit_status_error => integer,
          :post_comment_error => integer
        }

  def put_state(state) do
    GenServer.call(BorsNG.GitHub, {:put_state, state})
  end

  def get_state do
    GenServer.call(BorsNG.GitHub, {:get_state})
  end

  def init(:ok) do
    {:ok, %{}}
  end

  def handle_call({:put_state, state}, _from, _) do
    {:reply, :ok, state}
  end

  def handle_call({:get_state}, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:advisory, type, conn, args, deadline}, from, state) do
    if deadline > System.monotonic_time(:millisecond),
      do: handle_call({type, conn, args}, from, state),
      else: {:reply, {:error, :advisory_request}, state}
  end

  def handle_call({type, t, args}, _from, state) do
    {res, state} = do_handle_call(type, t, args, state)
    {:reply, res, state}
  end

  def handle_call(:get_app, _from, state) do
    {:reply, {:ok, "https://github.com/apps/bors-foobar"}, state}
  end

  def handle_call(:get_installation_list, _from, state) do
    list =
      state
      |> Enum.flat_map(fn
        {{{:installation, installation_number}, _repo_number}, _data} ->
          [installation_number]

        {{:installation, installation_number}, _data} ->
          [installation_number]

        _ ->
          []
      end)
      |> Enum.uniq()

    {:reply, {:ok, list}, state}
  end

  # Error injection, counted down like `get_pr_files_error`. The tuple shape
  # matters: the real server answers any non-200 with
  # `{:error, :get_pr, status, pr_xref}` (`server.ex`, `do_handle_call(:get_pr,
  # ...)`), and a call that never reaches it comes back as
  # `{:error, :github_call_timeout, :get_pr}`. The bare `{:error, :get_pr}`
  # below is a mock-only shape, so a caller that only handles *it* looks
  # correct under test and raises in production. Reach for this knob when a
  # test needs a realistic failed read.
  def do_handle_call(:get_commit_tree, conn, {sha}, state) do
    {{:ok, get_in(state, [conn, :commit_trees, sha]) || sha}, state}
  end

  def do_handle_call(:local_git_mode, conn, {}, state) do
    {{:ok, get_in(state, [conn, :local_git_mode]) || "off"}, state}
  end

  def do_handle_call(:git_token, _conn, {}, state), do: {{:ok, "test-token"}, state}

  def do_handle_call(:merge_backend_snapshot, conn, {}, state) do
    {get_in(state, [conn, :backend_snapshot]) || {:error, :unavailable}, state}
  end

  def do_handle_call(:merge_candidates, conn, {}, state) do
    {get_in(state, [conn, :merge_candidates]) || {:error, :unavailable}, state}
  end

  def do_handle_call(:merge_candidates_page, conn, {cursor}, state) do
    result =
      case get_in(state, [conn, :merge_candidate_pages, cursor]) do
        nil ->
          case get_in(state, [conn, :merge_candidates]) do
            {:ok, candidates} -> {:ok, %{candidates: candidates, next_cursor: nil}}
            other -> other || {:error, :unavailable}
          end

        page ->
          page
      end

    {result, state}
  end

  def do_handle_call(:release_queue_tail, conn, {selected_at, _deadline}, state) do
    case get_in(state, [conn, :backend_snapshot]) do
      {:ok, %{backend: "bors", updated_at: ^selected_at}} ->
        case get_in(state, [conn, :handoff_results]) do
          [{:ok, _entry} = result | rest] ->
            state = put_in(state, [conn, :handoff_results], rest)
            {:ok, snapshot} = get_in(state, [conn, :backend_snapshot])

            state =
              put_in(
                state,
                [conn, :backend_snapshot],
                {:ok, %{snapshot | github_count: snapshot.github_count - 1}}
              )

            {result, state}

          [result | rest] ->
            {result, put_in(state, [conn, :handoff_results], rest)}

          _ ->
            {{:done, :empty}, state}
        end

      _ ->
        {{:error, :backend_changed}, state}
    end
  end

  def do_handle_call(:get_eligibility_checks, conn, {head}, state) do
    {get_in(state, [conn, :eligibility_checks, head]) || {:ok, []}, state}
  end

  def do_handle_call(:get_pr_merge_base, conn, {pr, head}, state) do
    {get_in(state, [conn, :merge_bases, {pr, head}]) || {:error, :unavailable}, state}
  end

  def do_handle_call(:reset_scope, conn, {head}, state) do
    repo = Map.fetch!(state, conn)

    statuses =
      Map.update(
        Map.get(repo, :statuses, %{}),
        head,
        %{"scope" => :running},
        &Map.put(&1, "scope", :running)
      )

    {:ok, Map.put(state, conn, Map.put(repo, :statuses, statuses))}
  end

  def do_handle_call(:dispatch_pr_build, conn, {pr}, state) do
    repo = Map.fetch!(state, conn)

    case Map.get(repo, :pr_build_dispatch_error) do
      nil ->
        {:ok,
         Map.put(
           state,
           conn,
           Map.put(repo, :pr_build_dispatches, [pr | Map.get(repo, :pr_build_dispatches, [])])
         )}

      error ->
        {error, state}
    end
  end

  def do_handle_call(:dispatch_reconcile, conn, {}, state) do
    {:ok, put_in(state, [conn, :reconcile_dispatched], true)}
  end

  def do_handle_call(
        :get_pr,
        repo_conn,
        params,
        %{get_pr_error: 0} = state
      ) do
    do_handle_call(:get_pr, repo_conn, params, %{state | :get_pr_error => nil})
  end

  def do_handle_call(
        :get_pr,
        _repo_conn,
        {pr_xref},
        %{get_pr_error: n} = state
      )
      when is_integer(n) and n > 0 do
    {{:error, :get_pr, 502, pr_xref}, %{state | :get_pr_error => n - 1}}
  end

  def do_handle_call(:get_pr, repo_conn, {pr_xref}, state) do
    with(
      {:ok, repo} <- Map.fetch(state, repo_conn),
      {:ok, pulls} <- Map.fetch(repo, :pulls),
      do: Map.fetch(pulls, pr_xref)
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:error, :get_pr}, state}
    end
  end

  def do_handle_call(
        :batch_read,
        _conn,
        {_repository, _numbers, mode},
        %{batch_read_error: {mode, error}} = state
      ),
      do: {error, state}

  def do_handle_call(:public_user, {token, _repo_id}, {login}, state) do
    state = Map.update(state, :public_user_calls, [login], &[login | &1])
    do_handle_call(:get_user_by_login, token, {login}, state)
  end

  def do_handle_call(:batch_read, conn, {_repository, numbers, mode}, state) do
    state = Map.update(state, :batch_read_calls, [{mode, numbers}], &[{mode, numbers} | &1])

    Enum.reduce_while(numbers, {{:ok, %{}}, state}, fn number, {{:ok, acc}, state} ->
      case do_handle_call(:get_pr, conn, {number}, state) do
        {{:ok, pr}, state} ->
          value =
            if mode == :identity,
              do: pr,
              else: get_in(state, [:batch_read_metadata, number]) || :rest

          {:cont, {{:ok, Map.put(acc, number, value)}, state}}

        {error, state} ->
          {:halt, {error, state}}
      end
    end)
  end

  def do_handle_call(:cleanup_policy, conn, {commit}, state) do
    case do_handle_call(:get_file, conn, {commit, "bors.toml"}, state) do
      {{:ok, nil}, state} -> do_handle_call(:get_file, conn, {commit, ".github/bors.toml"}, state)
      result -> result
    end
  end

  def do_handle_call(:close_merged_pr, _conn, _params, %{close_pr_status: status} = state),
    do: {{:error, :close_merged_pr, status}, Map.delete(state, :close_pr_status)}

  def do_handle_call(:close_merged_pr, conn, {number, title}, state) do
    case do_handle_call(:get_pr, conn, {number}, state) do
      {{:ok, pr}, state} ->
        case do_handle_call(:update_pr, conn, %{pr | state: :closed, title: title}, state) do
          {{:ok, _}, state} -> {:ok, state}
          other -> other
        end

      {error, state} ->
        {error, state}
    end
  end

  def do_handle_call(:update_pr, repo_conn, pr, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, pulls} <- Map.fetch(repo, :pulls) do
      pulls = %{pulls | pr.number => pr}
      repo = %{repo | pulls: pulls}
      state = %{state | repo_conn => repo}
      {{:ok, pr}, state}
    end
    |> case do
      {{:ok, _}, _} = res -> res
      _ -> {{:error, :update_pr}, state}
    end
  end

  def do_handle_call(:update_pr_base, repo_conn, pr, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, pulls} <- Map.fetch(repo, :pulls) do
      pulls = %{pulls | pr.number => pr}
      repo = %{repo | pulls: pulls}
      state = %{state | repo_conn => repo}
      {{:ok, pr}, state}
    end
    |> case do
      {{:ok, _}, _} = res -> res
      _ -> {{:error, :update_pr}, state}
    end
  end

  def do_handle_call(
        :get_pr_files,
        repo_conn,
        params,
        %{get_pr_files_error: 0} = state
      ) do
    do_handle_call(
      :get_pr_files,
      repo_conn,
      params,
      %{state | :get_pr_files_error => nil}
    )
  end

  def do_handle_call(
        :get_pr_files,
        _repo_conn,
        {pr_xref},
        %{get_pr_files_error: n} = state
      )
      when is_integer(n) and n > 0 do
    {{:error, :get_pr_files, 502, pr_xref}, %{state | :get_pr_files_error => n - 1}}
  end

  def do_handle_call(:get_pr_files, repo_conn, {pr_xref}, state) do
    case get_in(state, [repo_conn, :pr_files, pr_xref]) do
      # Explicit per-PR file list (a plain list of filenames).
      filenames when is_list(filenames) ->
        {{:ok, Enum.map(filenames, &%BorsNG.GitHub.File{filename: &1})}, state}

      # Legacy CODEOWNERS shape: :files keyed by the staging commit "Z".
      _ ->
        with {:ok, repo} <- Map.fetch(state, repo_conn),
             {:ok, files} <- Map.fetch(repo, :files),
             {:ok, z_files} <- Map.fetch(files, "Z") do
          files =
            Enum.map(z_files, fn {k, _} ->
              %BorsNG.GitHub.File{filename: k}
            end)

          {{:ok, files}, state}
        else
          _ -> {{:error, :get_pr_files}, state}
        end
    end
  end

  def do_handle_call(:get_pr_compare, repo_conn, {base, head}, state) do
    # Tests put a :compare map on the repo keyed by `{base, head}` whose value
    # is a list of filenames changed between those two refs. Setting
    # :compare_error makes the call fail, to exercise the :unverifiable paths.
    repo = Map.get(state, repo_conn, %{})

    if Map.get(repo, :compare_error, false) do
      {{:error, :get_pr_compare, 502, {base, head}}, state}
    else
      case repo |> Map.get(:compare, %{}) |> Map.fetch({base, head}) do
        {:ok, filenames} ->
          files = Enum.map(filenames, &%BorsNG.GitHub.File{filename: &1})
          {{:ok, files}, state}

        :error ->
          {{:ok, []}, state}
      end
    end
  end

  def do_handle_call(:failure_base_changes, repo_conn, {base, head}, state) do
    result =
      get_in(state, [repo_conn, :failure_base_changes, {base, head}]) || {:error, :unavailable}

    {result, state}
  end

  def do_handle_call(:compare_status, repo_conn, {base, head}, state) do
    # Tests put a :compare_status map on the repo keyed by `{base, head}`
    # whose value is :ahead | :behind | :identical | :diverged. A missing
    # entry is an error (callers are expected to fail closed); setting
    # :compare_error makes the call fail like a GitHub outage.
    repo = Map.get(state, repo_conn, %{})

    if Map.get(repo, :compare_error, false) do
      {{:error, :compare_status, 502, {base, head}}, state}
    else
      case repo |> Map.get(:compare_status, %{}) |> Map.fetch({base, head}) do
        {:ok, status} -> {{:ok, status}, state}
        :error -> {{:error, :compare_status}, state}
      end
    end
  end

  def do_handle_call(:get_staging_base, conn, {head}, state) do
    case get_in(state, [conn, :commits, head, :parents]) do
      [base | _] -> {{:ok, base}, state}
      _ -> {{:error, :staging_base_unavailable}, state}
    end
  end

  def do_handle_call(:post_bump_failure, conn, {head, _batch}, state) do
    repo = Map.get(state, conn, %{})
    statuses = Map.get(repo, :statuses, %{})

    statuses =
      Map.update(
        statuses,
        head,
        %{"bump-integration" => :error},
        &Map.put(&1, "bump-integration", :error)
      )

    {:ok, Map.put(state, conn, Map.put(repo, :statuses, statuses))}
  end

  def do_handle_call(:get_pr_commits, repo_conn, {pr_xref}, state) do
    with(
      {:ok, repo} <- Map.fetch(state, repo_conn),
      {:ok, pr_commits} <- Map.fetch(repo, :pr_commits),
      do: Map.fetch(pr_commits, pr_xref)
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:error, :get_pr_commits}, state}
    end
  end

  def do_handle_call(:get_installation_repos, installation_conn, {}, state) do
    with(
      {:ok, installation} <- Map.fetch(state, installation_conn),
      {:ok, repos} <- Map.fetch(installation, :repos),
      do: {:ok, repos}
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:error, :get_open_prs}, state}
    end
  end

  def do_handle_call(:get_open_prs, repo_conn, {}, state) do
    with(
      {:ok, repo} <- Map.fetch(state, repo_conn),
      {:ok, pulls} <- Map.fetch(repo, :pulls),
      do: {:ok, Map.values(pulls) |> Enum.filter(&(&1.state == :open))}
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:error, :get_open_prs}, state}
    end
  end

  def do_handle_call(:get_open_prs_with_base, repo_conn, {base}, state) do
    with(
      {:ok, repo} <- Map.fetch(state, repo_conn),
      {:ok, pulls} <- Map.fetch(repo, :pulls),
      do: {
        :ok,
        Map.values(pulls)
        |> Enum.filter(&(&1.state == :open))
        |> Enum.filter(&(&1.base_ref == base))
      }
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:error, :get_open_prs_with_base}, state}
    end
  end

  def do_handle_call(:push, repo_conn, {sha, to}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, branches} <- Map.fetch(repo, :branches),
         push_errors <- Map.get(repo, :push_errors, %{}) do
      case Map.fetch(push_errors, to) do
        {:ok, error} ->
          {{:error, :push, error[:error_code], error[:response]}, state}

        _ ->
          branches = %{branches | to => sha}
          repo = %{repo | branches: branches}
          state = %{state | repo_conn => repo}
          {{:ok, sha}, state}
      end
    end
    |> case do
      {{:ok, _}, _} = res -> res
      {{:error, :push, _, _}, _} = res -> res
      _ -> {{:error, :push}, state}
    end
  end

  def do_handle_call(:get_branch, repo_conn, {from}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, branches} <- Map.fetch(repo, :branches) do
      sha =
        case branches[from] do
          nil -> from
          sha -> sha
        end

      if is_nil(branches[from]) and String.starts_with?(from, "staging-candidate-"),
        do: {{:error, :get_branch, 404, "missing"}, state},
        else: {{:ok, %{commit: sha, tree: sha}}, state}
    end
    |> case do
      {{:ok, _}, _} = res -> res
      {{:error, :get_branch, 404, _}, _} = res -> res
      _ -> {{:error, :get_branch}, state}
    end
  end

  def do_handle_call(:delete_branch, repo_conn, {branch}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, branches} <- Map.fetch(repo, :branches) do
      branches = Map.delete(branches, branch)
      repo = %{repo | branches: branches}
      state = %{state | repo_conn => repo}
      {:ok, state}
    end
    |> case do
      {:ok, _} = res -> res
      _ -> {{:error, :get_branch}, state}
    end
  end

  def do_handle_call(:merge_branch, _, _, %{merge_conflict: 0} = state) do
    {{:ok, :conflict}, state}
  end

  def do_handle_call(:merge_branch, repo_conn, params, %{merge_conflict: n} = state)
      when is_integer(n) do
    {result, state} =
      do_handle_call(
        :merge_branch,
        repo_conn,
        params,
        %{state | :merge_conflict => nil}
      )

    {result, %{state | :merge_conflict => n - 1}}
  end

  def do_handle_call(
        :merge_branch,
        repo_conn,
        {%{
           from: from,
           to: to,
           commit_message: _commit_message
         }},
        state
      ) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, branches} <- Map.fetch(repo, :branches) do
      base = branches[to]

      head =
        case branches[from] do
          nil -> from
          head -> head
        end

      # Fixtures using real-width SHAs can supply a fixed assembled tree;
      # the default concatenation keeps small symbolic Git graphs readable.
      nsha = state[:merge_tree] || base <> head
      branches = %{branches | to => nsha}
      repo = %{repo | branches: branches}
      state = %{state | repo_conn => repo}
      {{:ok, %{commit: nsha, tree: nsha}}, state}
    end
    |> case do
      {{:ok, _}, _} = res -> res
      _ -> {{:error, :merge_branch}, state}
    end
  end

  def do_handle_call(
        :create_commit,
        repo_conn,
        params,
        %{create_commit_error: 0} = state
      ) do
    do_handle_call(
      :create_commit,
      repo_conn,
      params,
      %{state | :create_commit_error => nil}
    )
  end

  def do_handle_call(
        :create_commit,
        _repo_conn,
        _params,
        %{create_commit_error: n} = state
      )
      when is_integer(n) and n > 0 do
    error = {:error, :create_commit, 502, "Bad Gateway", "MOCK-502"}
    {error, %{state | :create_commit_error => n - 1}}
  end

  def do_handle_call(
        :create_commit,
        repo_conn,
        {%{
           tree: tree,
           parents: parents,
           commit_message: commit_message,
           committer: _committer
         }},
        state
      ) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, commits} <- Map.fetch(repo, :commits) do
      nsha = tree

      commits =
        Map.put(commits, nsha, %{
          parents: parents,
          commit_message: commit_message
        })

      repo = %{repo | commits: commits}
      state = %{state | repo_conn => repo}
      {{:ok, nsha}, state}
    end
    |> case do
      {{:ok, _}, _} = res -> res
      _ -> {{:error, :create_commit}, state}
    end
  end

  def do_handle_call(
        :synthesize_commit,
        repo_conn,
        {%{
           branch: branch,
           tree: tree,
           parents: parents,
           commit_message: commit_message,
           committer: _committer
         }},
        state
      ) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, branches} <- Map.fetch(repo, :branches),
         {:ok, commits} <- Map.fetch(repo, :commits) do
      nsha =
        parents
        |> Enum.reverse()
        |> Enum.reduce(&<>/2)

      ^nsha = tree

      commits =
        Map.put(commits, nsha, %{
          parents: parents,
          commit_message: commit_message
        })

      branches = Map.put(branches, branch, nsha)
      repo = %{repo | branches: branches}
      repo = %{repo | commits: commits}
      state = %{state | repo_conn => repo}
      {{:ok, nsha}, state}
    end
    |> case do
      {{:ok, _}, _} = res -> res
      _ -> {{:error, :synthesize_commit}, state}
    end
  end

  def do_handle_call(:force_push, repo_conn, {sha, to}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, branches} <- Map.fetch(repo, :branches) do
      branches = Map.put(branches, to, sha)
      repo = %{repo | branches: branches}
      state = %{state | repo_conn => repo}
      {{:ok, sha}, state}
    end
    |> case do
      {{:ok, _}, _} = res -> res
      _ -> {{:error, :force_push}, state}
    end
  end

  def do_handle_call(
        :get_commit_status,
        repo_conn,
        params,
        %{get_commit_status_error: 0} = state
      ) do
    do_handle_call(
      :get_commit_status,
      repo_conn,
      params,
      %{state | :get_commit_status_error => nil}
    )
  end

  def do_handle_call(
        :get_commit_status,
        _repo_conn,
        _params,
        %{get_commit_status_error: n} = state
      )
      when is_integer(n) and n > 0 do
    {{:error, :get_commit_status, 502}, %{state | :get_commit_status_error => n - 1}}
  end

  def do_handle_call(:active_staging_count, conn, {}, state) do
    {{:ok, get_in(state, [conn, :active_staging_count]) || 0}, state}
  end

  def do_handle_call(:cancellation_lookup, conn, {p}, state) do
    if state[:cancellation_lookup_error] do
      {state[:cancellation_lookup_error], state}
    else
      {{:ok, get_in(state, [conn, :staging_runs, {p["head_sha"], p["batch_id"]}])}, state}
    end
  end

  def do_handle_call(:cancel_candidate_run, _conn, args, state) do
    if hook = state[:cancellation_hook], do: hook.()

    case BorsNG.Worker.CandidateCancellation.authority(elem(args, 0)) do
      {:ok, _} ->
        {state[:cancel_candidate_run_error] || :ok,
         Map.update(state, :cancellations, [args], &[args | &1])}

      error ->
        {error, state}
    end
  end

  def do_handle_call(:staging_run, conn, {head, _base, batch, _branch, _since}, state) do
    {{:ok, get_in(state, [conn, :staging_runs, {head, batch}])}, state}
  end

  def do_handle_call(:recovered_staging_finalization, conn, {run_id, attempt}, state) do
    jobs = get_in(state, [conn, :staging_jobs, {run_id, attempt}])
    {{:ok, BorsNG.Worker.StagingFailure.recovered_finalization?(jobs)}, state}
  end

  def do_handle_call(
        :staging_dispatch_seen,
        repo_conn,
        {_repo, head, _base, batch, _branch, _since},
        state
      ) do
    if state[:dispatch_lookup_error],
      do: {{:error, :dispatch_lookup}, state},
      else: {{:ok, {head, batch} in (get_in(state, [repo_conn, :dispatches]) || [])}, state}
  end

  def do_handle_call(
        :dispatch_staging,
        _repo_conn,
        _params,
        %{dispatch_error_before_accept: true} = state
      ) do
    {{:error, :dispatch_staging, 429, "rate limited"}, state}
  end

  def do_handle_call(
        :dispatch_staging,
        repo_conn,
        {_repo, head, _base, batch, _members, _branch},
        state
      ) do
    state =
      update_in(state, [repo_conn, :dispatches], fn current -> [{head, batch} | current || []] end)

    if hook = state[:dispatch_after_accept_hook] do
      send(hook, {:dispatch_accepted, self()})

      receive do
        :release_dispatch -> :ok
      after
        5000 -> :ok
      end
    end

    if state[:dispatch_error_after_accept],
      do: {{:error, :dispatch_staging, 502, "lost response"}, state},
      else: {:ok, state}
  end

  def do_handle_call(:staging_failure, repo_conn, {head, _, _, _, _}, state) do
    outcome = get_in(state, [repo_conn, :staging_failures, head]) || :conclusive
    {{:ok, outcome}, state}
  end

  def do_handle_call(:get_commit_status, repo_conn, {sha}, state) do
    with(
      {:ok, repo} <- Map.fetch(state, repo_conn),
      {:ok, statuses} <- Map.fetch(repo, :statuses),
      do: {:ok, statuses[sha] || %{}}
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:error, :get_commit_status}, state}
    end
  end

  def do_handle_call(
        :get_labels,
        repo_conn,
        params,
        %{get_labels_error: 0} = state
      ) do
    do_handle_call(
      :get_labels,
      repo_conn,
      params,
      %{state | :get_labels_error => nil}
    )
  end

  def do_handle_call(
        :get_labels,
        _repo_conn,
        {_issue_xref},
        %{get_labels_error: n} = state
      )
      when is_integer(n) and n > 0 do
    {{:error, :get_labels, 502, "Bad Gateway"}, %{state | :get_labels_error => n - 1}}
  end

  def do_handle_call(:get_labels, repo_conn, {issue_xref}, state) do
    with(
      {:ok, repo} <- Map.fetch(state, repo_conn),
      {:ok, labels} <- Map.fetch(repo, :labels),
      do: {:ok, labels[issue_xref]}
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:ok, []}, state}
    end
  end

  def do_handle_call(:add_labels, repo_conn, {issue_xref, new_labels}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn) do
      labels = Map.get(repo, :labels, %{})
      existing = labels[issue_xref] || []
      merged = Enum.uniq(existing ++ new_labels)
      repo = Map.put(repo, :labels, Map.put(labels, issue_xref, merged))
      {:ok, %{state | repo_conn => repo}}
    end
    |> case do
      {:ok, state} -> {:ok, state}
      _ -> {{:error, :add_labels}, state}
    end
  end

  def do_handle_call(:remove_label, repo_conn, {issue_xref, label}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn) do
      labels = Map.get(repo, :labels, %{})
      remaining = (labels[issue_xref] || []) -- [label]
      repo = Map.put(repo, :labels, Map.put(labels, issue_xref, remaining))
      {:ok, %{state | repo_conn => repo}}
    end
    |> case do
      {:ok, state} -> {:ok, state}
      _ -> {{:error, :remove_label}, state}
    end
  end

  def do_handle_call(
        :list_issues_by_label,
        _repo_conn,
        {_label},
        %{list_issues_by_label_error: true} = state
      ) do
    {{:error, :list_issues_by_label, 502, "Bad Gateway"}, state}
  end

  def do_handle_call(:list_issues_by_label, repo_conn, {label}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn) do
      result =
        repo
        |> Map.get(:labels, %{})
        |> Enum.filter(fn {_issue_xref, names} -> label in (names || []) end)
        |> Enum.map(fn {issue_xref, names} -> {issue_xref, names} end)

      {{:ok, result}, state}
    end
    |> case do
      {{:ok, _}, _} = res -> res
      _ -> {{:error, :list_issues_by_label}, state}
    end
  end

  def do_handle_call(
        :get_reviews,
        repo_conn,
        params,
        %{get_reviews_error: 0} = state
      ) do
    do_handle_call(
      :get_reviews,
      repo_conn,
      params,
      %{state | :get_reviews_error => nil}
    )
  end

  def do_handle_call(
        :get_reviews,
        _repo_conn,
        {_issue_xref, _sha},
        %{get_reviews_error: n} = state
      )
      when is_integer(n) and n > 0 do
    {{:error, :get_reviews, 502, "Bad Gateway"}, %{state | :get_reviews_error => n - 1}}
  end

  def do_handle_call(:get_reviews, repo_conn, {issue_xref, sha}, state) do
    review_type =
      if is_nil(sha) do
        :reviews
      else
        :up_to_date_reviews
      end

    with(
      {:ok, repo} <- Map.fetch(state, repo_conn),
      {:ok, reviews} <- Map.fetch(repo, review_type),
      do: {:ok, reviews[issue_xref]}
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:ok, %{"APPROVED" => 0, "CHANGES_REQUESTED" => 0}}, state}
    end
  end

  def do_handle_call(:get_reviews, repo_conn, {issue_xref}, state) do
    do_handle_call(:get_reviews, repo_conn, {issue_xref, nil}, state)
  end

  def do_handle_call(:get_file, repo_conn, {branch, path}, state) do
    with(
      {:ok, repo} <- Map.fetch(state, repo_conn),
      {:ok, files} <- Map.fetch(repo, :files),
      do: {:ok, files[branch][path]}
    )
    |> case do
      {:ok, _} = res -> {res, state}
      _ -> {{:error, :get_file}, state}
    end
  end

  def do_handle_call(:get_repo_tree, repo_conn, {branch}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, files} <- Map.fetch(repo, :files),
         branch_files when is_map(branch_files) <- Map.get(files, branch) do
      # A repo can list branches whose tree is truncated to mirror GitHub's
      # recursive-tree cap; the real server returns this terminal tuple.
      if branch in Map.get(repo, :truncated_trees, []) do
        {{:ok, {:truncated, branch}}, state}
      else
        {{:ok, Map.keys(branch_files)}, state}
      end
    else
      _ -> {{:error, :get_repo_tree}, state}
    end
  end

  def do_handle_call(:notice_comments_since, conn, {number, _since}, state),
    do: do_handle_call(:get_pr_comments, conn, {number}, state)

  def do_handle_call(:resolve_source_request, conn, {pr, head, base}, state) do
    if error = get_in(state, [conn, :resolve_source_request_error]) do
      {{:error, error}, state}
    else
      marker = "<!--tauceti-rebase:v1 #{head}-->"
      comments = get_in(state, [conn, :comments, pr]) || []
      updated = Enum.map(comments, &String.replace(&1, marker, "Superseded against main #{base}"))
      {:ok, put_in(state, [conn, :comments, pr], updated)}
    end
  end

  def do_handle_call(:get_pr_comments, repo_conn, {number}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, comments} <- Map.fetch(repo, :comments),
         {:ok, c} <- Map.fetch(comments, number) do
      {{:ok, c}, state}
    else
      _ -> {{:ok, []}, state}
    end
  end

  def do_handle_call(:post_comment, repo_conn, params, %{post_comment_error: 0} = state) do
    do_handle_call(:post_comment, repo_conn, params, %{state | :post_comment_error => nil})
  end

  def do_handle_call(:post_comment, _repo_conn, _params, %{post_comment_error: n} = state)
      when is_integer(n) and n > 0 do
    {{:error, :post_comment}, %{state | :post_comment_error => n - 1}}
  end

  def do_handle_call(:post_comment, repo_conn, {number, body}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, comments} <- Map.fetch(repo, :comments),
         {:ok, c} <- Map.fetch(comments, number) do
      c = [body | c]
      comments = %{comments | number => c}
      repo = %{repo | comments: comments}
      state = %{state | repo_conn => repo}
      {:ok, state}
    end
    |> case do
      {:ok, state} -> {:ok, state}
      _ -> {{:error, :post_comment}, state}
    end
  end

  def do_handle_call(
        :post_commit_status,
        repo_conn,
        params,
        %{post_commit_status_error: 0} = state
      ) do
    do_handle_call(:post_commit_status, repo_conn, params, %{
      state
      | :post_commit_status_error => nil
    })
  end

  def do_handle_call(
        :post_commit_status,
        _repo_conn,
        _params,
        %{post_commit_status_error: n} = state
      )
      when is_integer(n) and n > 0 do
    {{:error, :post_commit_status}, %{state | :post_commit_status_error => n - 1}}
  end

  def do_handle_call(
        :post_commit_status,
        repo_conn,
        {sha, status, _, _},
        state
      ) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, statuses} <- Map.fetch(repo, :statuses) do
      sha_statuses =
        case Map.has_key?(statuses, sha) do
          false ->
            %{"bors" => status}

          true ->
            statuses
            |> Map.fetch!(sha)
            |> Map.put("bors", status)
        end

      statuses = Map.put(statuses, sha, sha_statuses)
      repo = %{repo | statuses: statuses}
      state = %{state | repo_conn => repo}
      {:ok, state}
    end
    |> case do
      {:ok, state} -> {:ok, state}
      _ -> {{:error, :post_commit_status}, state}
    end
  end

  def do_handle_call(:get_collaborators_by_repo, repo_conn, {}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, users} <- Map.fetch(repo, :collaborators) do
      {{:ok, users}, state}
    else
      _ ->
        {{:error, :get_collaborators_by_repo}, state}
    end
  end

  def do_handle_call(:repository_admin, repo_conn, {actor}, state) do
    with {:ok, repo} <- Map.fetch(state, repo_conn),
         {:ok, permissions} <- Map.fetch(repo, :admin_permissions) do
      {Map.get(permissions, actor, {:ok, false}), state}
    else
      _ -> {{:error, :repository_admin_unavailable}, state}
    end
  end

  def do_handle_call(
        :get_user_by_login,
        _token,
        {login},
        state
      ) do
    with(
      {:ok, users} <- Map.fetch(state, :users),
      do: {:ok, users[login]}
    )
    |> case do
      # A test maps a login to `:error` to simulate a failed lookup.
      {:ok, :error} -> {{:error, :get_user_by_login}, state}
      {:ok, user} -> {{:ok, user}, state}
      _ -> {{:ok, nil}, state}
    end
  end
end
