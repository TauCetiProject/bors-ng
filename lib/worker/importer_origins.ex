defmodule BorsNG.Worker.ImporterOrigins do
  @moduledoc """
  One import hop for lint findings in modules no member changed.

  The environment lint also checks transitive importers of the changed modules,
  so a finding can sit in an unchanged file. Its direct project imports are read
  at the exact candidate commit (the existing offline provider lookup, which
  refuses a file that differs between base and candidate); when exactly one
  member changed one of those imports, that member becomes the scheduling
  suspect. Zero or several matching members, or any incomplete read, leave the
  observation as it was. Scheduling hint only, never proof of individual failure.
  """
  alias BorsNG.Worker.{FailureIsolation, PrivateOrigins}

  @policy "importer-hop-v1"
  def policy, do: @policy

  @doc """
  The unchanged module a finding names: a lint finding by listed path or private
  declaration, or a compile failure in a module no member changed (its import
  of a changed module broke it).
  """
  def origin(%{"kind" => "environment_linter"} = observation) do
    case observation["paths"] do
      [path] when is_binary(path) -> if project_path?(path), do: path
      [] -> PrivateOrigins.path(observation)
      _ -> nil
    end
  end

  def origin(%{"kind" => "compiler_error", "paths" => [path]}) when is_binary(path),
    do: if(project_path?(path), do: path)

  def origin(_), do: nil

  @doc "Unowned finding paths worth one hop, bounded like the other source lookups."
  def paths(hints, %{"complete" => true, "jobs" => jobs}) when is_list(jobs) do
    paths =
      jobs
      |> Enum.flat_map(& &1["observations"])
      |> Enum.map(&origin/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.filter(&(owners(hints, &1) == []))
      |> Enum.uniq()
      |> Enum.sort()

    if FailureIsolation.ownership_complete?(hints) and length(paths) in 1..8,
      do: {:ok, paths},
      else: :unavailable
  rescue
    _ -> :unavailable
  end

  def paths(_, _), do: :unavailable

  @doc """
  Rewrite each hopped observation to the member's changed import, recording
  the hop as `attributed_via`, when the lookup is complete and bound to this
  exact candidate, run and membership.
  """
  def resolve(
        hints,
        logs,
        binding,
        %{
          "complete" => true,
          "binding" => binding,
          "authority" => "retry_only",
          "proof_authority" => false,
          "files" => files
        } = lookup
      ) do
    fields = ~w(candidate_id generation base_sha members_digest commit)

    with {:ok, paths} <- paths(hints, logs),
         true <- is_map(hints["provenance"]) and Enum.all?(fields, &(not is_nil(binding[&1]))),
         true <- Map.take(hints["provenance"], fields) == Map.take(binding, fields),
         true <-
           hints["provenance"]["members"] ==
             Enum.map(hints["changed_files"], &Map.take(&1, ["pr", "head_sha"])),
         true <- Enum.map(logs["jobs"], & &1["job_id"]) == binding["job_ids"],
         true <- binding["workflow"] == ".github/workflows/pr-build.yml",
         true <-
           Enum.all?([binding["run_id"], binding["run_attempt"]], &(is_integer(&1) and &1 > 0)),
         true <- is_list(files) and Enum.map(files, & &1["path"]) == paths,
         true <- Enum.all?(files, &file?/1),
         true <- byte_size(Jason.encode!(lookup)) <= 64 * 1024 do
      imports = Map.new(files, &{&1["path"], Enum.map(&1["providers"], fn p -> p["path"] end)})

      Map.update!(logs, "jobs", fn jobs ->
        Enum.map(jobs, fn job ->
          Map.update!(job, "observations", fn observations ->
            Enum.map(observations, &hop(hints, imports, &1))
          end)
        end)
      end)
    else
      _ -> logs
    end
  rescue
    _ -> logs
  end

  def resolve(_, logs, _, _), do: logs

  defp hop(hints, imports, observation) do
    with path when is_binary(path) <- origin(observation),
         {:ok, modules} <- Map.fetch(imports, path),
         [{pr, hit}] <- matches(hints, modules) do
      hopped =
        observation
        |> Map.put("paths", hit)
        |> Map.put("origin", "importer_hop")
        |> Map.put("attributed_via", %{"path" => path, "imports" => hit, "pr" => pr})
        |> Map.delete("unknown")

      if Map.has_key?(observation, "unknown"),
        do: Map.put(hopped, "origin_unknown", observation["unknown"]),
        else: hopped
    else
      _ -> observation
    end
  end

  # Members whose changed files include one of the module's direct imports.
  defp matches(hints, modules) do
    for member <- hints["changed_files"] || [],
        hit = Enum.sort(Enum.filter(modules, &(&1 in (member["paths"] || [])))),
        hit != [],
        do: {member["pr"], hit}
  end

  defp owners(hints, path) do
    dependencies = Map.get(hints, "dependencies_complete", hints["complete"]) == true

    for member <- hints["changed_files"] || [],
        path in (member["paths"] || []) or
          (dependencies and path in (member["dependency_paths"] || [])),
        do: member["pr"]
  end

  defp file?(%{"path" => path, "blob" => blob, "bytes" => bytes, "providers" => providers}) do
    project_path?(path) and sha?(blob) and is_integer(bytes) and bytes > 0 and
      bytes <= 256 * 1024 and is_list(providers) and length(providers) <= 128 and
      Enum.all?(providers, fn
        %{"path" => p, "blob" => b} -> project_path?(p) and sha?(b)
        _ -> false
      end)
  end

  defp file?(_), do: false

  defp sha?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{40}\z/, value)

  defp project_path?(path),
    do:
      is_binary(path) and byte_size(path) <= 512 and
        Regex.match?(~r/\ATauCeti(?:\/[A-Za-z_][A-Za-z0-9_'\-]*)+\.lean\z/, path)
end
