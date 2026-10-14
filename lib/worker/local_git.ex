defmodule BorsNG.Worker.LocalGit do
  @moduledoc "Bounded, disposable bare mirrors behind the immutable candidate contract."
  @behaviour BorsNG.Worker.CandidateAssembler
  alias BorsNG.Worker.CandidateAssembler
  alias BorsNG.GitHub

  # Scope the initial rollout to the repository with immutable base policy and
  # trusted staging dispatch. A failed variable read disables the optimization.
  def mode(project, branch, conn) do
    if BorsNG.Worker.ManualApproval.scoped?(project, branch) do
      case GitHub.local_git_mode(conn) do
        {:ok, mode} when mode in ["shadow", "local"] -> mode
        _ -> "off"
      end
    else
      "off"
    end
  end

  @impl true
  def assemble(candidate, context) do
    expected = candidate.snapshot["members"]
    members = context.members

    if Enum.map(expected, &{&1["pr"], &1["assembly_head_sha"]}) ==
         Enum.map(members, &{&1.pr, &1.source_sha}) and
         Enum.all?(expected, &(&1["head_sha"] == &1["assembly_head_sha"])) do
      request = %{
        operation: "assemble",
        diagnostics: BorsNG.Worker.FailureIsolation.enabled?(),
        identity: CandidateAssembler.identity(candidate),
        members: Enum.map(members, &Map.put(&1, :head_sha, &1.source_sha)),
        date: Map.get(context, :date, DateTime.to_iso8601(DateTime.utc_now()))
      }

      case run(context, request) do
        %{"status" => "ok", "result" => result} = evidence ->
          result = %{
            candidate_id: result["candidate_id"],
            generation: result["generation"],
            base_sha: result["base_sha"],
            members_digest: result["members_digest"],
            commit: result["commit"],
            tree: result["tree"],
            evidence: Map.drop(evidence, ["result", "status"])
          }

          {:ok, result}

        %{"status" => "conflict", "details" => details} ->
          {:conflict, details}

        %{"details" => details} ->
          {:error, details}

        _ ->
          {:error, %{kind: "invalid_helper_result"}}
      end
    else
      {:error, %{kind: "input_identity_mismatch"}}
    end
  end

  # Caller performs live/identity checks and persists CI intent first. No
  # temporary remote refs exist to leak after process/container restart.
  def push(result, context, expected) do
    run(context, %{
      operation: "publish",
      commit: result.commit,
      ref: "refs/heads/" <> Map.get(context, :staging_ref, "staging"),
      expected: expected
    })
  end

  def promote(batch, conn) do
    context = %{repository: batch.project.name, conn: conn}

    case run(context, %{
           operation: "promote",
           commit: batch.commit,
           expected: batch.base_sha,
           ref: "refs/heads/main"
         }) do
      %{"status" => "ok"} -> {:ok, batch.commit}
      _ -> {:error, :push, 503, "Exact-base Git promotion unavailable"}
    end
  end

  defp run(context, request) do
    input =
      Path.join(
        System.tmp_dir!(),
        "bors-git-#{Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)}.json"
      )

    try do
      request =
        Map.merge(request, %{
          root: System.get_env("BORS_GIT_MIRROR_ROOT", "/tmp/bors-git-mirrors"),
          repository: context.repository,
          max_bytes: 2 * 1024 * 1024 * 1024,
          timeout: 600
        })

      {:ok, file} = File.open(input, [:write, :exclusive])
      File.chmod!(input, 0o600)
      IO.binwrite(file, Jason.encode!(request))
      File.close(file)

      with {:ok, token} <- GitHub.git_token(context.conn),
           {output, 0} <-
             System.cmd("python3", [Application.app_dir(:bors, "priv/local_git.py"), input],
               env: [{"BORS_GIT_TOKEN", token}],
               stderr_to_stdout: false
             ),
           {:ok, result} <- Jason.decode(output) do
        result
      else
        _ -> %{"status" => "error", "details" => %{"kind" => "helper_or_auth_failed"}}
      end
    rescue
      _ -> %{"status" => "error", "details" => %{"kind" => "helper_unavailable"}}
    after
      File.rm(input)
    end
  end
end
