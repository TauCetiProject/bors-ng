defmodule BorsNG.Worker.FailureDiagnostics do
  @moduledoc "Candidate-bound, durable observations of untrusted terminal compiler output."
  import Ecto.Query
  alias BorsNG.Database.{CandidateEvent, Repo}
  alias BorsNG.Worker.{CandidateAssembler, CandidateJournal, FailureIsolation, LocalGit}

  @steps [
    "Preflight exact-candidate import headers",
    "Build exact candidate under bwrap with workflow-pinned audits, offline"
  ]

  # Runs in the existing per-project orchestration executor, outside the shared
  # GitHub server/scheduler. A persisted unavailable result is final for this
  # immutable candidate/run attempt, including after process restart.
  def observe(
        batch,
        evidence,
        conn,
        collector \\ &collect/2,
        providers \\ nil,
        sources \\ &LocalGit.collision_sources/4
      ) do
    candidate = CandidateJournal.current(batch)
    hints = FailureIsolation.hints(candidate)

    if FailureIsolation.enabled?() and not is_nil(candidate) and is_map(hints) and
         FailureIsolation.ownership_complete?(hints) and
         length(candidate.snapshot["members"]) > 1 and eligible?(batch, evidence) and
         fresh_run?(candidate, evidence) do
      binding = binding(candidate, batch.commit, evidence)

      unless observation(candidate, binding) do
        jobs = job_ids(evidence)

        result =
          if length(jobs) in 1..2,
            do: collector.(conn, jobs),
            else: %{"complete" => false, "reason" => "ambiguous_failed_jobs"}

        CandidateJournal.event(candidate, "failure_diagnostics", "bors.terminal_logs", %{
          binding: binding,
          result: result,
          collision_sources:
            collision_lookup(candidate, batch, evidence, hints, result, conn, sources),
          provider_lookup:
            provider_lookup(candidate, batch, evidence, hints, result, conn, providers),
          authority: "retry_only",
          proof_authority: false
        })
      end
    end

    :ok
  end

  defp fresh_run?(candidate, evidence) do
    with value when is_binary(value) <- evidence[:run_created_at],
         {:ok, created, _} <- DateTime.from_iso8601(value) do
      DateTime.to_unix(created, :microsecond) >= candidate.observed_at
    else
      _ -> false
    end
  end

  defp eligible?(batch, evidence) do
    evidence[:diagnostic] == nil and evidence[:conclusion] == "failure" and
      evidence[:workflow] == ".github/workflows/pr-build.yml" and
      evidence[:identity] ==
        "bors branch=#{batch.into_branch} batch=#{batch.id} head=#{batch.commit} base=#{batch.base_sha}" and
      is_integer(evidence[:run_id]) and evidence[:run_id] > 0 and
      is_integer(evidence[:run_attempt]) and evidence[:run_attempt] > 0
  end

  defp job_ids(evidence) do
    (evidence[:failed_steps] || [])
    |> Enum.filter(&(&1[:job_name] == "sandboxed-build" and &1[:name] in @steps))
    |> Enum.map(& &1[:job_id])
    |> Enum.filter(&(is_integer(&1) and &1 > 0))
    |> Enum.uniq()
    |> Enum.sort()
  end

  def binding(candidate, commit, evidence) do
    CandidateAssembler.identity(candidate)
    |> Map.merge(%{
      commit: commit,
      run_id: evidence[:run_id] || evidence["run_id"],
      run_attempt: evidence[:run_attempt] || evidence["run_attempt"],
      job_ids: job_ids(normalize_evidence(evidence)),
      workflow: evidence[:workflow] || evidence["workflow"]
    })
    |> Jason.encode!()
    |> Jason.decode!()
  end

  defp normalize_evidence(evidence) do
    %{
      failed_steps:
        Enum.map(evidence[:failed_steps] || evidence["failed_steps"] || [], fn step ->
          %{
            job_id: step[:job_id] || step["job_id"],
            job_name: step[:job_name] || step["job_name"],
            name: step[:name] || step["name"]
          }
        end)
    }
  end

  defp observation(candidate, binding) do
    Repo.all(
      from(e in CandidateEvent,
        where:
          e.candidate_id == ^candidate.id and e.kind == "failure_diagnostics" and
            e.source == "bors.terminal_logs",
        order_by: [desc: e.id]
      )
    )
    |> Enum.find(&(&1.details["binding"] == binding))
  end

  # Only the latest trusted classifier observation can select an observation.
  # A changed generation/head/base/run/attempt/job cannot reuse another's logs.
  def suspects(candidate, hints) do
    case diagnosis(candidate, hints) do
      {:ok, prs, _} -> {:ok, prs}
      other -> other
    end
  end

  def diagnosis(candidate, hints) do
    with {:ok, observation} <- bound_observation(candidate) do
      case map_paths(hints, observation.details["result"]) do
        {:ok, prs} ->
          {:ok, prs,
           %{
             "binding" => observation.details["binding"],
             "diagnostics" => observation.details["result"]
           }}

        other ->
          other
      end
    end
  end

  # Provider hints deliberately never enter diagnosis/2, which authorizes the
  # stronger provisional singleton repair handoff. Only retry grouping uses them.
  def retry_suspects(candidate, hints) do
    case suspects(candidate, hints) do
      {:ok, prs} ->
        {:ok, prs, "compiler_diagnostics"}

      _ ->
        with {:ok, observation} <- bound_observation(candidate),
             {:ok, prs, policy} <-
               map_retry_providers(
                 hints,
                 observation.details["result"],
                 observation.details["binding"],
                 observation.details["provider_lookup"]
               ) do
          {:ok, prs, policy}
        else
          _ -> :unavailable
        end
    end
  end

  # Source identity is separate from advisory provider ownership. Only this
  # bound direct collision exception can bypass component widening.
  def base_collision(candidate, hints) do
    with {:ok, observation} <- bound_observation(candidate),
         {:ok, pr} <-
           map_base_collision(
             hints,
             observation.details["result"],
             observation.details["binding"],
             observation.details["collision_sources"]
           ),
         {:ok, ^observation} <- bound_observation(candidate, true),
         true <- current_collision_context?(candidate, hints) do
      {:ok, pr,
       %{
         "binding" => observation.details["binding"],
         "sources" => observation.details["collision_sources"]
       }}
    else
      _ -> :unavailable
    end
  rescue
    _ -> :unavailable
  end

  defp current_collision_context?(candidate, hints) do
    bound = CandidateJournal.commit_binding(candidate)
    expected = Enum.map(candidate.snapshot["members"], &Map.take(&1, ["pr", "head_sha"]))

    provenance =
      CandidateAssembler.identity(candidate)
      |> Map.merge(%{commit: bound["commit"], tree: bound["tree"], members: expected})
      |> Jason.encode!()
      |> Jason.decode!()

    hints["provenance"] == provenance and
      CandidateAssembler.validate_result(
        candidate,
        Map.merge(CandidateAssembler.identity(candidate), %{
          commit: bound["commit"],
          tree: bound["tree"]
        })
      ) == :ok
  end

  defp collision_binding_current?(candidate, commit, evidence) do
    live = Repo.get(BorsNG.Database.Batch, candidate.batch_id)

    normalized =
      Map.merge(normalize_evidence(evidence), %{
        diagnostic: evidence["diagnostic"],
        conclusion: evidence["conclusion"],
        workflow: evidence["workflow"],
        identity: evidence["identity"],
        run_id: evidence["run_id"],
        run_attempt: evidence["run_attempt"],
        run_created_at: evidence["run_created_at"]
      })

    not is_nil(live) and live.commit == commit and live.base_sha == candidate.snapshot["base_sha"] and
      eligible?(live, normalized) and fresh_run?(candidate, normalized)
  end

  defp collision_lookup(candidate, batch, evidence, hints, result, conn, sources) do
    with {:ok, pair} <- collision_paths(hints, result),
         true <- current_collision_context?(candidate, hints),
         true <- batch.base_sha == candidate.snapshot["base_sha"],
         true <- Enum.map(result["jobs"], & &1["job_id"]) == job_ids(evidence) do
      lookup =
        sources.(batch.project, conn, binding(candidate, batch.commit, evidence), pair.paths)

      if is_map(lookup) and byte_size(Jason.encode!(lookup)) <= 64 * 1024,
        do: lookup,
        else: %{"complete" => false, "reason" => "source_output_limit"}
    else
      _ -> %{"complete" => false, "reason" => "ineligible_collision"}
    end
  rescue
    _ -> %{"complete" => false, "reason" => "source_unavailable"}
  end

  defp bound_observation(candidate, strict? \\ false) do
    with false <- is_nil(candidate),
         event when not is_nil(event) <-
           Repo.one(
             from(e in CandidateEvent,
               where:
                 e.candidate_id == ^candidate.id and e.kind == "failure_classified" and
                   e.source == "bors.trusted_failure",
               order_by: [desc: e.id],
               limit: 1
             )
           ),
         %{"classification" => "candidate_failure", "evidence" => evidence, "commit" => commit} <-
           event.details,
         true <- evidence["diagnostic"] == nil,
         true <- CandidateJournal.commit_binding(candidate)["commit"] == commit,
         true <- not strict? or collision_binding_current?(candidate, commit, evidence),
         observation when not is_nil(observation) <-
           observation(candidate, binding(candidate, commit, evidence)) do
      {:ok, observation}
    else
      _ -> :unavailable
    end
  end

  defp provider_lookup(candidate, batch, evidence, hints, result, conn, providers) do
    bound = CandidateJournal.commit_binding(candidate)
    identity = CandidateAssembler.identity(candidate)
    expected = Enum.map(candidate.snapshot["members"], &Map.take(&1, ["pr", "head_sha"]))

    provenance =
      Map.merge(identity, %{commit: batch.commit, tree: bound["tree"], members: expected})
      |> Jason.encode!()
      |> Jason.decode!()

    with :unavailable <- map_paths(hints, result),
         true <- batch.base_sha == candidate.snapshot["base_sha"],
         true <- hints["provenance"] == provenance,
         true <-
           Enum.map(hints["changed_files"] || [], &Map.take(&1, ["pr", "head_sha"])) == expected,
         :ok <-
           CandidateAssembler.validate_result(
             candidate,
             Map.merge(
               identity,
               %{commit: batch.commit, tree: bound["tree"]}
             )
           ),
         {:ok, paths} <- provider_paths(hints, result) do
      bound = binding(candidate, batch.commit, evidence)

      lookup =
        if is_nil(providers),
          do: LocalGit.retry_providers(batch.project, conn, bound, paths, hints["changed_files"]),
          else: providers.(batch.project, conn, bound, paths)

      if is_map(lookup) and byte_size(Jason.encode!(lookup)) <= 64 * 1024,
        do: lookup,
        else: %{"complete" => false, "reason" => "provider_output_limit"}
    else
      _ -> %{"complete" => false, "reason" => "ineligible_or_unmapped"}
    end
  rescue
    _ -> %{"complete" => false, "reason" => "provider_unavailable"}
  end

  # Restrict the first rollout to precise compiler errors whose paths all lack
  # existing ownership. Mixed/unknown/audit findings retain conservative fallback.
  def provider_paths(hints, %{"complete" => true, "jobs" => jobs}) when is_list(jobs) do
    observations = Enum.flat_map(jobs, & &1["observations"])
    paths = observations |> Enum.flat_map(& &1["paths"]) |> Enum.uniq() |> Enum.sort()
    changed = hints["changed_files"] || []
    dependencies = Map.get(hints, "dependencies_complete", hints["complete"]) == true

    if FailureIsolation.ownership_complete?(hints) and observations != [] and
         Enum.all?(observations, &(&1["kind"] == "compiler_error" and &1["paths"] != [])) and
         length(paths) in 1..8 and Enum.all?(paths, &project_path?/1) and
         Enum.all?(paths, fn path ->
           Enum.all?(changed, fn member ->
             path not in member["paths"] and
               (not dependencies or path not in (member["dependency_paths"] || []))
           end)
         end),
       do: {:ok, paths},
       else: :unavailable
  rescue
    _ -> :unavailable
  end

  def provider_paths(_, _), do: :unavailable

  @provider_limits %{
    "max_depth" => 5,
    "max_headers" => 64,
    "max_file_bytes" => 262_144,
    "max_source_bytes" => 524_288,
    "max_paths" => 128,
    "max_evidence_bytes" => 65_536,
    "total_timeout_ms" => 5000
  }

  def map_retry_providers(hints, logs, binding, %{"policy" => "bounded_depth_v1"} = lookup) do
    case map_bounded_providers(hints, logs, binding, lookup) do
      {:ok, prs} -> {:ok, prs, "bounded_provider_hint"}
      _ -> :unavailable
    end
  end

  def map_retry_providers(_, _, _, %{"policy" => _}), do: :unavailable

  def map_retry_providers(hints, logs, binding, lookup) do
    case map_providers(hints, logs, binding, lookup) do
      {:ok, prs} -> {:ok, prs, "import_provider_hint"}
      _ -> :unavailable
    end
  end

  # Validate the entire bounded graph, including every residual branch. No
  # resource-truncated graph or root without a changed frontier can guide retries.
  def map_bounded_providers(hints, logs, binding, lookup) do
    with %{
           "policy" => "bounded_depth_v1",
           "complete" => false,
           "bounded_complete" => true,
           "full_closure_complete" => false,
           "binding" => ^binding,
           "authority" => "retry_only",
           "proof_authority" => false,
           "limits" => @provider_limits,
           "nodes" => nodes
         }
         when is_list(nodes) <- lookup,
         true <- byte_size(Jason.encode!(lookup)) <= 65_536,
         {:ok, roots} <- provider_paths(hints, logs),
         true <- lookup["roots"] == roots,
         true <- length(nodes) in 1..128,
         changed = hints["changed_files"] |> Enum.flat_map(& &1["paths"]) |> MapSet.new(),
         :ok <-
           validate_provider_graph(nodes, Enum.map(roots, &{&1, 0}), MapSet.new(roots), changed),
         expanded = Enum.filter(nodes, &is_list(&1["providers"])),
         residual = Enum.filter(nodes, &(&1["stop"] == "depth_limit")),
         frontier = Enum.filter(nodes, &(&1["stop"] == "changed_frontier")),
         true <- lookup["residual_frontier"] == Enum.map(residual, & &1["path"]),
         true <- lookup["changed_frontier"] == Enum.map(frontier, & &1["path"]),
         true <- lookup["headers"] == length(expanded) and length(expanded) <= 64,
         bytes = Enum.sum(Enum.map(expanded, & &1["bytes"])),
         true <- lookup["source_bytes"] == bytes and bytes <= 524_288,
         true <- is_integer(lookup["elapsed_ms"]) and lookup["elapsed_ms"] in 0..5000,
         graph = Map.new(nodes, &{&1["path"], &1}),
         true <- Enum.all?(roots, &reaches_frontier?([&1], graph, MapSet.new())) do
      paths = MapSet.new(Enum.map(frontier, & &1["path"]))

      owners =
        hints["changed_files"]
        |> Enum.filter(fn m -> Enum.any?(m["paths"], &MapSet.member?(paths, &1)) end)
        |> Enum.map(& &1["pr"])
        |> Enum.uniq()

      if owners != [], do: {:ok, owners}, else: :unavailable
    else
      _ -> :unavailable
    end
  rescue
    _ -> :unavailable
  end

  defp validate_provider_graph([], [], _, _), do: :ok

  defp validate_provider_graph([node | rest], [{path, depth} | queue], seen, changed) do
    true = node["path"] == path and node["depth"] == depth and project_path?(path)
    true = sha?(node["blob"]) and is_integer(node["bytes"]) and node["bytes"] in 0..262_144

    cond do
      MapSet.member?(changed, path) ->
        true = depth > 0 and node["stop"] == "changed_frontier" and node["providers"] == nil
        validate_provider_graph(rest, queue, seen, changed)

      depth == 5 ->
        true = node["stop"] == "depth_limit" and node["providers"] == nil
        validate_provider_graph(rest, queue, seen, changed)

      depth in 0..4 ->
        providers = node["providers"]
        true = node["stop"] == nil and node["base_blob"] == node["blob"]
        true = is_list(providers) and providers == Enum.sort(Enum.uniq(providers))
        true = Enum.all?(providers, &project_path?/1)
        fresh = Enum.reject(providers, &MapSet.member?(seen, &1))
        seen = Enum.reduce(fresh, seen, &MapSet.put(&2, &1))
        true = MapSet.size(seen) <= 128
        validate_provider_graph(rest, queue ++ Enum.map(fresh, &{&1, depth + 1}), seen, changed)
    end
  end

  defp validate_provider_graph(_, _, _, _), do: :unavailable

  defp reaches_frontier?([], _, _), do: false

  defp reaches_frontier?([path | rest], graph, seen) do
    if MapSet.member?(seen, path) do
      reaches_frontier?(rest, graph, seen)
    else
      node = Map.fetch!(graph, path)

      node["stop"] == "changed_frontier" or
        reaches_frontier?(rest ++ (node["providers"] || []), graph, MapSet.put(seen, path))
    end
  end

  def map_providers(
        hints,
        logs,
        binding,
        %{
          "complete" => true,
          "binding" => binding,
          "files" => files,
          "authority" => "retry_only",
          "proof_authority" => false
        } = lookup
      )
      when is_list(files) do
    with true <- byte_size(Jason.encode!(lookup)) <= 64 * 1024,
         {:ok, paths} <- provider_paths(hints, logs),
         true <- Enum.map(files, & &1["path"]) == paths,
         true <-
           Enum.all?(files, fn file ->
             sha?(file["blob"]) and is_integer(file["bytes"]) and file["bytes"] in 0..262_144 and
               is_list(file["providers"]) and length(file["providers"]) <= 128 and
               Enum.all?(file["providers"], fn p ->
                 project_path?(p["path"]) and sha?(p["blob"])
               end)
           end),
         true <- Enum.sum(Enum.map(files, & &1["bytes"])) <= 512 * 1024 do
      owners =
        Enum.map(files, fn file ->
          providers = Enum.map(file["providers"], & &1["path"])

          hints["changed_files"]
          |> Enum.filter(fn member ->
            Enum.any?(providers, &(&1 in member["paths"]))
          end)
          |> Enum.map(& &1["pr"])
        end)

      if Enum.all?(owners, &(&1 != [])),
        do: {:ok, owners |> List.flatten() |> Enum.uniq()},
        else: :unavailable
    else
      _ -> :unavailable
    end
  rescue
    _ -> :unavailable
  end

  def map_providers(_, _, _, _), do: :unavailable

  defp sha?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{40}\z/, value)

  defp project_path?(path),
    do:
      is_binary(path) and byte_size(path) <= 512 and
        Regex.match?(~r/\ATauCeti(?:\/[A-Za-z_][A-Za-z0-9_'\-]*)+\.lean\z/, path)

  def map_paths(%{"changed_files" => changed} = hints, %{
        "complete" => true,
        "jobs" => jobs
      }) do
    observations = Enum.flat_map(jobs, & &1["observations"])

    mapped =
      Enum.map(observations, fn observation ->
        owners =
          Enum.map(observation["paths"], fn path ->
            Enum.filter(changed, fn member ->
              path in member["paths"] or
                (Map.get(hints, "dependencies_complete", hints["complete"]) == true and
                   path in (member["dependency_paths"] || []))
            end)
            |> Enum.map(& &1["pr"])
          end)

        # Both sides of an audit collision are retained. An unchanged base
        # declaration has no changed owner; every changed side is suspected.
        if observation["kind"] == "duplicate_declaration" or Enum.all?(owners, &(&1 != [])),
          do: List.flatten(owners),
          else: []
      end)

    if FailureIsolation.ownership_complete?(hints) and mapped != [] and
         Enum.all?(mapped, &(&1 != [])),
       do: {:ok, mapped |> List.flatten() |> Enum.uniq()},
       else: :unavailable
  end

  def map_paths(_, _), do: :unavailable

  @doc "Exact one-owner collision paths, excluding imports and mixed diagnostics."
  def collision_paths(hints, %{"complete" => true, "jobs" => jobs}) when is_list(jobs) do
    with true <-
           hints["ownership_complete"] == true and hints["complete"] == true and
             hints["dependencies_complete"] == true and hints["missing_imports"] == [],
         true <- length(jobs) in 1..2,
         true <-
           Enum.all?(
             jobs,
             &(is_integer(&1["job_id"]) and &1["job_id"] > 0 and
                 is_integer(&1["bytes"]) and &1["bytes"] in 1..16_777_216)
           ),
         true <- length(Enum.uniq_by(jobs, & &1["job_id"])) == length(jobs),
         true <- Enum.all?(jobs, &(is_list(&1["observations"]) and &1["observations"] != [])),
         observations <- Enum.flat_map(jobs, & &1["observations"]),
         true <- Enum.all?(observations, &(&1["kind"] == "duplicate_declaration")),
         [paths] <- observations |> Enum.map(&Enum.sort(&1["paths"])) |> Enum.uniq(),
         true <- length(paths) == 2 and length(Enum.uniq(paths)) == 2,
         true <- Enum.all?(paths, &project_path?/1),
         owners <-
           Enum.map(paths, fn path ->
             {path, for(m <- hints["changed_files"], path in m["paths"], do: m["pr"])}
           end),
         [{changed, [pr]}] <- Enum.filter(owners, fn {_, owners} -> owners != [] end),
         [{unchanged, []}] <- Enum.filter(owners, fn {_, owners} -> owners == [] end),
         true <-
           Enum.all?(hints["changed_files"], &(unchanged not in (&1["dependency_paths"] || []))) do
      {:ok, %{pr: pr, paths: paths, changed: changed, unchanged: unchanged}}
    else
      _ -> :unavailable
    end
  rescue
    _ -> :unavailable
  end

  def collision_paths(_, _), do: :unavailable

  @doc "Pure base collision mapping; caller verifies current candidate/classifier provenance."
  def map_base_collision(
        hints,
        %{"jobs" => jobs} = logs,
        binding,
        %{
          "complete" => true,
          "policy" => "collision-source-identity-v1",
          "binding" => binding,
          "authority" => "retry_only",
          "proof_authority" => false,
          "files" => files
        } = sources
      ) do
    fields = ~w(candidate_id generation base_sha members_digest commit)
    provenance = hints["provenance"]

    with {:ok, pair} <- collision_paths(hints, logs),
         true <- is_map(provenance) and Enum.all?(fields, &(not is_nil(binding[&1]))),
         true <- Map.take(provenance, fields) == Map.take(binding, fields),
         true <- is_binary(provenance["tree"]) and provenance["tree"] != "",
         true <-
           provenance["members"] ==
             Enum.map(hints["changed_files"], &Map.take(&1, ["pr", "head_sha"])),
         true <- binding["workflow"] == ".github/workflows/pr-build.yml",
         true <-
           Enum.all?([binding["run_id"], binding["run_attempt"]], &(is_integer(&1) and &1 > 0)),
         true <- Enum.map(jobs, & &1["job_id"]) == binding["job_ids"],
         true <-
           Enum.all?(
             jobs,
             &(is_integer(&1["job_id"]) and &1["job_id"] > 0 and
                 is_integer(&1["bytes"]) and &1["bytes"] in 1..16_777_216)
           ),
         true <- byte_size(Jason.encode!(sources)) <= 64 * 1024,
         true <- Enum.map(files, & &1["path"]) == pair.paths,
         true <-
           Enum.all?(
             files,
             &(source_entry?(&1["candidate"]) and
                 (is_nil(&1["base"]) or source_entry?(&1["base"])))
           ),
         unchanged <- Enum.find(files, &(&1["path"] == pair.unchanged)),
         true <- unchanged["base"] == unchanged["candidate"],
         changed <- Enum.find(files, &(&1["path"] == pair.changed)),
         true <-
           is_nil(changed["base"]) or changed["base"]["blob"] != changed["candidate"]["blob"],
         entries <-
           Enum.flat_map(files, &[&1["base"], &1["candidate"]])
           |> Enum.reject(&is_nil/1)
           |> Enum.uniq(),
         true <- Enum.sum(Enum.map(entries, & &1["bytes"])) <= 512 * 1024 do
      {:ok, pair.pr}
    else
      _ -> :unavailable
    end
  rescue
    _ -> :unavailable
  end

  def map_base_collision(_, _, _, _), do: :unavailable

  defp source_entry?(%{"blob" => blob, "bytes" => bytes}),
    do: sha?(blob) and is_integer(bytes) and bytes in 0..262_144

  defp source_entry?(_), do: false

  defp collect(conn, jobs) do
    input =
      Path.join(
        System.tmp_dir!(),
        "bors-diagnostics-#{Base.encode16(:crypto.strong_rand_bytes(16))}.json"
      )

    try do
      {{_, _}, repo_id} = conn
      {:ok, file} = File.open(input, [:write, :exclusive])
      File.chmod!(input, 0o600)
      IO.binwrite(file, Jason.encode!(%{repository_id: repo_id, job_ids: jobs}))
      File.close(file)

      with {:ok, token} <- BorsNG.GitHub.git_token(conn),
           {output, 0} <-
             System.cmd(
               "python3",
               [Application.app_dir(:bors, "priv/failure_diagnostics.py"), input],
               env: [{"BORS_DIAGNOSTICS_TOKEN", token}],
               stderr_to_stdout: false
             ),
           {:ok, result} <- Jason.decode(output) do
        result
      else
        _ -> %{"complete" => false, "reason" => "collector_unavailable"}
      end
    rescue
      _ -> %{"complete" => false, "reason" => "collector_unavailable"}
    after
      File.rm(input)
    end
  end
end
