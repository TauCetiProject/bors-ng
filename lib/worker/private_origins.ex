defmodule BorsNG.Worker.PrivateOrigins do
  @moduledoc "Verified private-module origins; scheduling hints, never proof of individual failure."
  alias BorsNG.Worker.FailureIsolation
  @policy "private-origin-source-v1"
  def policy, do: @policy

  # Lean's private prefix carries a module followed by a numeric uniqueness
  # counter. Quoted/ambiguous module spellings are deliberately unsupported.
  def path(%{
        "kind" => "environment_linter",
        "paths" => [],
        "unknown" => reason,
        "declaration" => name
      })
      when is_binary(name) do
    if reason in ~w(unsupported_origin private_or_generated missing_origin missing_range) do
      case Regex.run(
             ~r/\A_private\.(TauCeti(?:\.[A-Za-z_][A-Za-z0-9_']*)+)\.[0-9]{1,20}\.[^\x00-\x1f\x7f]{1,512}\z/u,
             name,
             capture: :all_but_first
           ) do
        [module] when byte_size(module) <= 506 -> String.replace(module, ".", "/") <> ".lean"
        _ -> nil
      end
    end
  end

  def path(_), do: nil

  def unlocated?(%{"complete" => true, "jobs" => jobs}) do
    Enum.any?(jobs, fn job -> Enum.any?(job["observations"], &(not is_nil(path(&1)))) end)
  rescue
    _ -> false
  end

  def unlocated?(_), do: false

  def paths(hints, %{"complete" => true, "jobs" => jobs}) when is_list(jobs) do
    paths =
      jobs
      |> Enum.flat_map(& &1["observations"])
      |> Enum.map(&path/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.sort()

    if FailureIsolation.ownership_complete?(hints) and length(paths) in 1..8 and
         Enum.all?(paths, fn path ->
           length(Enum.filter(hints["changed_files"], &(path in &1["paths"]))) == 1
         end),
       do: {:ok, paths},
       else: :unavailable
  rescue
    _ -> :unavailable
  end

  def paths(_, _), do: :unavailable

  def resolve(
        hints,
        logs,
        binding,
        %{
          "complete" => true,
          "policy" => @policy,
          "binding" => binding,
          "authority" => "retry_only",
          "proof_authority" => false,
          "files" => files
        } = sources
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
         true <- Enum.map(files, & &1["path"]) == paths,
         true <- Enum.all?(files, &entry?(&1["candidate"])),
         true <- Enum.sum(Enum.map(files, & &1["candidate"]["bytes"])) <= 512 * 1024,
         true <- byte_size(Jason.encode!(sources)) <= 64 * 1024 do
      Map.update!(logs, "jobs", fn jobs ->
        Enum.map(jobs, fn job ->
          Map.update!(job, "observations", fn observations ->
            Enum.map(observations, fn observation ->
              case path(observation) do
                path when is_binary(path) ->
                  observation
                  |> Map.put("paths", [path])
                  |> Map.put("origin", "verified_private_module")
                  |> Map.put("origin_unknown", observation["unknown"])
                  |> Map.delete("unknown")

                _ ->
                  observation
              end
            end)
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

  defp entry?(%{"blob" => blob, "bytes" => bytes}),
    do:
      is_binary(blob) and Regex.match?(~r/\A[0-9a-f]{40}\z/, blob) and
        is_integer(bytes) and bytes > 0 and bytes <= 256 * 1024

  defp entry?(_), do: false
end
