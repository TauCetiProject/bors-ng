defmodule BorsNG.Worker.MergeEligibility do
  @moduledoc "Consumes App-owned policy checks inside the project's serialized batcher."
  import Ecto.Query
  require Logger
  alias BorsNG.Database.{Batch, LinkPatchBatch, Patch, Project, Repo}
  alias BorsNG.GitHub
  alias BorsNG.Worker.{Batcher, Syncer}

  @name "merge eligibility"
  @repo "TauCetiProject/TauCeti"
  @schema "tauceti-merge.eligibility/v1"
  @reviewer "tauceti-review-bot[bot]"
  @keep ~w(keep hold wip human do-not-close)

  def name, do: @name

  def trusted?(check) do
    case Integer.parse(System.get_env("TAUCETI_REVIEW_APP_ID") || "") do
      {id, ""} -> check["name"] == @name and get_in(check, ["app", "id"]) == id
      _ -> false
    end
  end

  def notification_pr(check) do
    with true <- trusted?(check),
         {:ok, data} when is_map(data) <- Jason.decode(check["external_id"] || ""),
         @repo <- data["repo"],
         pr when is_integer(pr) and pr > 0 <- data["pr"] do
      pr
    else
      _ -> nil
    end
  end

  # Fetch with filter=all: another App's check of the same name cannot hide ours.
  # The highest ID is authoritative, irrespective of webhook delivery order.
  def latest(checks, pr, head) do
    checks
    |> Enum.filter(&(trusted?(&1) and &1["head_sha"] == head and notification_pr(&1) == pr))
    |> Enum.max_by(& &1["id"], fn -> nil end)
    |> decode(pr, head)
  end

  defp decode(nil, _, _), do: {:ok, nil}

  defp decode(check, pr, head) do
    with {:ok, data} <- Jason.decode(check["external_id"] || ""),
         @schema <- data["schema"],
         @repo <- data["repo"],
         ^pr <- data["pr"],
         ^head <- check["head_sha"],
         "completed" <- check["status"],
         true <-
           is_boolean(data["eligible"]) and is_boolean(data["review_safe"]) and
             is_boolean(data["single"]),
         true <- not data["eligible"] or data["review_safe"],
         conclusion <-
           if(data["eligible"],
             do: "success",
             else: if(data["review_safe"], do: "neutral", else: "failure")
           ),
         ^conclusion <- check["conclusion"],
         true <- not data["eligible"] or sha?(data["merge_base_sha"]) do
      {:ok, Map.merge(data, %{"id" => check["id"], "head_sha" => head})}
    else
      _ -> {:error, :invalid_eligibility_check}
    end
  end

  defp sha?(s), do: is_binary(s) and Regex.match?(~r/\A[0-9a-f]{40}\z/, s)

  def reconcile(%Project{name: @repo} = project, pr_number) do
    conn = Project.installation_connection(project.repo_xref, Repo)

    with {:ok, pr} <- GitHub.get_pr(conn, pr_number) do
      patch = Repo.get_by(Patch, project_id: project.id, pr_xref: pr_number)
      # A missed push/close/retarget cancels the old approval even if the new
      # head has no eligibility check yet.
      if patch &&
           (patch.commit != pr.head_sha or pr.state != :open or pr.base_ref != "main" or pr.draft) do
        Batcher.do_handle_cast({:cancel, patch.id, :eligibility_changed}, project.id)
      end

      patch = Syncer.sync_patch(project.id, pr)

      with {:ok, checks} <- GitHub.get_eligibility_checks(conn, pr.head_sha),
           {:ok, decision} when not is_nil(decision) <- latest(checks, pr_number, pr.head_sha) do
        apply_decision(project, conn, pr, patch, decision)
      else
        _ -> :ok
      end
    else
      _ -> :ok
    end
  rescue
    error ->
      Logger.warning("merge eligibility deferred pr=#{pr_number}: #{Exception.message(error)}")
  end

  def reconcile(_, _), do: :ok

  defp apply_decision(project, conn, pr, patch, d) do
    cond do
      d["review_safe"] == false ->
        # Repeat cancellation is safe and repairs a crash between recording and cancelling.
        patch |> Patch.changeset(%{merge_eligibility_id: d["id"]}) |> Repo.update!()
        Batcher.do_handle_cast({:cancel, patch.id, :review_unsafe}, project.id)

      not d["eligible"] or pr.state != :open or pr.base_ref != "main" or pr.draft ->
        :ok

      patch.merge_eligibility_id == d["id"] ->
        :ok

      represented_or_terminal?(patch) ->
        :ok

      true ->
        with :ok <- BorsNG.MergeBackend.allow(project, "main", :admit),
             {:ok, labels} <- GitHub.get_labels(conn, pr.number),
             true <- MapSet.disjoint?(MapSet.new(labels), MapSet.new(@keep)),
             {:ok, merge_base} <- GitHub.get_pr_merge_base(conn, pr.number, pr.head_sha),
             true <- merge_base == d["merge_base_sha"] do
          # Durable intent and held approval are one write. Restart recovery can complete admission.
          patch =
            patch
            |> Patch.changeset(%{
              merge_eligibility_id: d["id"],
              merge_eligibility: d,
              is_single: d["single"],
              bundle_reviewer: @reviewer
            })
            |> Repo.update!()

          Logger.info(
            "merge eligibility admitted pr=#{pr.number} head=#{pr.head_sha} check=#{d["id"]}"
          )

          Batcher.do_handle_cast({:reviewed, patch.id, @reviewer}, project.id)
        else
          _ -> :ok
        end
    end
  end

  defp represented_or_terminal?(patch) do
    # Never loop failed heads or add a duplicate membership. Human retry remains available.
    Repo.exists?(
      from(l in LinkPatchBatch,
        join: b in Batch,
        on: b.id == l.batch_id,
        where: l.patch_id == ^patch.id and l.head_sha == ^patch.commit and b.state != :canceled
      )
    ) or
      not is_nil(patch.bundle_reviewer)
  end

  # The proof is reread at preflight, including held approvals recovered after a restart.
  # A neutral decision preserves an existing approval, as the review policy does today.
  def preflight(_conn, %Patch{merge_eligibility: nil}), do: :ok

  def preflight(conn, patch) do
    with {:ok, pr} <- GitHub.get_pr(conn, patch.pr_xref),
         true <-
           pr.state == :open and pr.base_ref == "main" and not pr.draft and
             pr.head_sha == patch.commit,
         {:ok, checks} <- GitHub.get_eligibility_checks(conn, patch.commit),
         {:ok, d} when not is_nil(d) <- latest(checks, patch.pr_xref, patch.commit),
         true <- d["review_safe"],
         {:ok, base} <- GitHub.get_pr_merge_base(conn, patch.pr_xref, patch.commit),
         true <- base == patch.merge_eligibility["merge_base_sha"] do
      :ok
    else
      false -> {:error, :review_unsafe}
      {:ok, nil} -> :waiting
      {:error, :invalid_eligibility_check} -> :waiting
      {:error, _, _, _} -> :waiting
      _ -> :waiting
    end
  end
end
