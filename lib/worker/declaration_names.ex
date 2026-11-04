defmodule BorsNG.Worker.DeclarationNames do
  @moduledoc """
  Names a PR adds, recorded from its own trusted build, compared between
  batch-mates before assembly.

  The post-build duplicate-declaration audit stays the authority. This module
  can only keep a known batch-mate collision out of a candidate; whenever a
  record is missing, incomplete, stale or over budget the unit is simply not
  checked, which is today's behaviour. Nothing here blocks admission, dispatch
  or assembly: the record is fetched by a background outbox job after
  admission, and the assembly check runs under its own small budget.
  """
  import Ecto.Query
  require Logger

  alias BorsNG.Database.{CandidateAdmission, Project, Repo}
  alias BorsNG.GitHub
  alias BorsNG.Worker.Orchestration.Outbox

  @budget_ms 2_000
  @max_modules 4_096
  @max_names 16_384

  @doc """
  `BORS_DECLARATION_COLLISIONS`: `off` (default) does nothing; `record` fetches
  and stores records without checking; `on` fetches and checks.
  """
  def mode do
    case System.get_env("BORS_DECLARATION_COLLISIONS") do
      "on" -> "on"
      "record" -> "record"
      _ -> "off"
    end
  end

  def enabled?, do: mode() == "on"
  def recording?, do: mode() in ["record", "on"]

  # --- record fetch after admission -------------------------------------

  @doc """
  Queue the background fetch for every link of a planning pass whose admission
  has no record yet. The pass itself treats those units as unknown; the next
  pass sees the stored record. Dedup per head; never raises.
  """
  def schedule(project_id, links) do
    if recording?() and Outbox.enabled?() do
      ids = Enum.map(links, & &1.id)

      Repo.all(from(a in CandidateAdmission, where: a.link_id in ^ids))
      |> Enum.each(fn a ->
        s = a.snapshot || %{}

        if is_nil(s["declaration_names"]) do
          Outbox.enqueue(
            project_id,
            "info",
            {:declaration_names, a.id},
            "declaration_names:#{project_id}:#{s["pr"]}:#{s["head_sha"]}"
          )
        end
      end)
    end

    :ok
  rescue
    _ -> :ok
  end

  @doc """
  Fetch and store the record for one admission. Every failure stores
  `unknown`; the job completes either way and is never repeated for the head.
  """
  def fetch(project_id, admission_id) do
    admission = Repo.get(CandidateAdmission, admission_id)

    if admission && admission.project_id == project_id &&
         is_nil((admission.snapshot || %{})["declaration_names"]) do
      project = Repo.get!(Project, project_id)
      conn = Project.installation_connection(project.repo_xref, Repo)
      head = admission.snapshot["head_sha"]

      record =
        try do
          case GitHub.pr_build_job(conn, head) do
            {:ok, %{job_id: job}} -> collect(conn, job, head)
            _ -> unknown("build_run_unavailable")
          end
        rescue
          _ -> unknown("fetch_failure")
        catch
          _, _ -> unknown("fetch_failure")
        end

      store(admission, record)
    end

    :ok
  end

  defp unknown(reason), do: %{"state" => "unknown", "reason" => reason}

  # Store on every admission of this exact head that lacks a record, so a
  # retry cloned before the fetch finished is covered too.
  defp store(admission, record) do
    snapshot = admission.snapshot
    head = snapshot["head_sha"]
    patch_id = snapshot["patch_id"]

    Repo.all(
      from(a in CandidateAdmission,
        where: a.project_id == ^admission.project_id and a.id >= ^admission.id
      )
    )
    |> Enum.filter(fn a ->
      s = a.snapshot || %{}
      s["head_sha"] == head and s["patch_id"] == patch_id and is_nil(s["declaration_names"])
    end)
    |> Enum.each(fn a ->
      a
      |> Ecto.Changeset.change(snapshot: Map.put(a.snapshot, "declaration_names", record))
      |> Repo.update!()
    end)
  end

  @doc false
  def collect(conn, job, head) do
    input =
      Path.join(
        System.tmp_dir!(),
        "bors-declaration-names-#{Base.encode16(:crypto.strong_rand_bytes(16))}.json"
      )

    try do
      {{_, _}, repo_id} = conn
      {:ok, file} = File.open(input, [:write, :exclusive])
      File.chmod!(input, 0o600)

      IO.binwrite(
        file,
        Jason.encode!(%{
          mode: "declaration_names",
          repository_id: repo_id,
          job_ids: [job],
          head: head,
          time_budget: 20
        })
      )

      File.close(file)

      with {:ok, token} <- GitHub.git_token(conn),
           {output, 0} <-
             System.cmd(
               "python3",
               [Application.app_dir(:bors, "priv/failure_diagnostics.py"), input],
               env: [{"BORS_DIAGNOSTICS_TOKEN", token}],
               stderr_to_stdout: false
             ),
           {:ok, %{"complete" => true, "record" => record}} <- Jason.decode(output),
           {:ok, validated} <- validate(record) do
        validated
      else
        _ -> unknown("collector_unavailable")
      end
    rescue
      _ -> unknown("collector_unavailable")
    after
      File.rm(input)
    end
  end

  @doc "The Elixir side trusts nothing: exact shape, bounds and name types again."
  def validate(%{"state" => state} = record) when state in ["unknown", "incomplete"] do
    {:ok, %{"state" => state, "reason" => to_string(record["reason"] || "")}}
  end

  def validate(%{"state" => state, "added" => added})
      when state in ["known", "empty"] and is_map(added) do
    names =
      Enum.flat_map(added, fn {module, list} ->
        if is_binary(module) and is_list(list), do: list, else: [:invalid]
      end)

    valid =
      map_size(added) <= @max_modules and length(names) <= @max_names and
        Enum.all?(names, &(is_binary(&1) and byte_size(&1) in 1..1024)) and
        state == "empty" == (names == [])

    if valid,
      do: {:ok, %{"state" => state, "added" => added}},
      else: {:ok, unknown("invalid_record")}
  end

  def validate(_), do: {:ok, unknown("invalid_record")}

  # --- the PR build that posted `build` ------------------------------------

  @doc "The run id behind the newest successful `build` status, if it is a trusted run URL."
  def build_run(statuses, repo) when is_list(statuses) do
    statuses
    |> Enum.filter(&is_map/1)
    |> Enum.uniq_by(& &1["context"])
    |> Enum.find(&(&1["context"] == "build"))
    |> case do
      %{"state" => "success", "target_url" => url} when is_binary(url) ->
        uri = URI.parse(url)
        prefix = "/#{repo}/actions/runs/"

        with true <- uri.scheme == "https" and uri.host == "github.com",
             true <- is_binary(uri.path) and String.starts_with?(uri.path, prefix),
             true <- is_nil(uri.query) and is_nil(uri.fragment),
             {id, ""} when id > 0 <- Integer.parse(String.replace_prefix(uri.path, prefix, "")) do
          {:lookup, id}
        else
          _ -> :unavailable
        end

      _ ->
        :unavailable
    end
  end

  def build_run(_, _), do: :unavailable

  # --- assembly check ---------------------------------------------------------

  @doc """
  Units to skip before the Git selection, in input order. A unit is checked
  only with a known record from this exact head; the first accepted unit that
  adds the same name in a different module names the collision. Remaining
  units are left unchecked once the budget is spent.
  """
  def collisions(units, records, now \\ &System.monotonic_time/1) do
    started = now.(:millisecond)

    {skips, _index, _cut} =
      Enum.reduce(units, {[], %{}, false}, fn unit, {skips, index, cut} ->
        cond do
          cut ->
            {skips, index, cut}

          now.(:millisecond) - started > @budget_ms ->
            Logger.warning("declaration_check_cut_short budget_ms=#{@budget_ms}")
            {skips, index, true}

          true ->
            case checkable(unit, records) do
              nil ->
                {skips, index, cut}

              {link, added} ->
                case first_collision(added, index) do
                  nil ->
                    {skips, index_names(index, link, added), cut}

                  {name, module, other} ->
                    skip = %{
                      "members" => [member(link)],
                      "conflict" =>
                        member(link)
                        |> Map.merge(%{
                          "kind" => "declaration_collision",
                          "name" => name,
                          "module" => module,
                          "other_pr" => other.pr,
                          "other_module" => other.module
                        })
                    }

                    {skips ++ [skip], index, cut}
                end
            end
        end
      end)

    skips
  end

  defp member(link), do: %{"pr" => link.patch.pr_xref, "head_sha" => link.head_sha}

  # A bundle, a changed head, or anything but a known record is unchecked.
  defp checkable([link], records) do
    with nil <- link.patch.bundle_id,
         %{"state" => state, "added" => added} when state in ["known", "empty"] and is_map(added) <-
           Map.get(records, link.id) do
      {link, added}
    else
      _ -> nil
    end
  end

  defp checkable(_, _), do: nil

  defp first_collision(added, index) do
    Enum.find_value(added, fn {module, names} ->
      Enum.find_value(names, fn name ->
        case Map.get(index, name) do
          %{module: other_module} = other when other_module != module -> {name, module, other}
          _ -> nil
        end
      end)
    end)
  end

  defp index_names(index, link, added) do
    Enum.reduce(added, index, fn {module, names}, acc ->
      Enum.reduce(names, acc, fn name, acc2 ->
        Map.put_new(acc2, name, %{pr: link.patch.pr_xref, module: module})
      end)
    end)
  end

  @doc "Records for the links of a batch, keyed by link id, from their admissions."
  def records(links) do
    ids = Enum.map(links, & &1.id)

    Repo.all(from(a in CandidateAdmission, where: a.link_id in ^ids))
    |> Enum.reduce(%{}, fn a, acc ->
      link = Enum.find(links, &(&1.id == a.link_id))
      s = a.snapshot || %{}

      if link && s["head_sha"] == link.head_sha && s["patch_id"] == link.patch_id &&
           is_map(s["declaration_names"]),
         do: Map.put(acc, a.link_id, s["declaration_names"]),
         else: acc
    end)
  end

  @doc "One neutral comment per collision; the PR stays approved and is tried again."
  def notify(project_id, conn, collision) do
    c = collision["conflict"]
    head = c["head_sha"]
    marker = "<!--tauceti-declaration-collision:v1 #{head} #{c["name"]} #{c["other_pr"]}-->"

    body =
      "Merge-queue notice for head `#{String.slice(head, 0, 7)}`: this PR declares `#{c["name"]}` " <>
        "in `#{c["module"]}`; ##{c["other_pr"]} (queued earlier) declares it in `#{c["other_module"]}`. " <>
        "Both cannot land together. Rename or move one declaration, then update the branch from `main`. " <>
        "This PR stays approved and is tried again when the other lands or either branch changes.\n\n#{marker}"

    if Outbox.enabled?() do
      Outbox.enqueue(
        project_id,
        "notice",
        {conn, c["pr"], body, marker},
        "declaration-collision:#{project_id}:#{c["pr"]}:#{head}:#{:erlang.phash2({c["name"], c["other_pr"]})}"
      )
    end

    :ok
  rescue
    _ -> :ok
  end
end
