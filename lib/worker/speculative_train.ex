defmodule BorsNG.Worker.SpeculativeTrain do
  @moduledoc "Depth-two scheduling, exact promotion and durable staging reservations."
  import Ecto.Query
  alias BorsNG.Database.{Batch, Candidate, CandidateEvent, LinkPatchBatch, Project, Repo}
  alias BorsNG.Worker.{CandidateAssembler, CandidateJournal}
  alias BorsNG.Worker.Batcher.{Divider, GetBorsToml}
  alias BorsNG.Worker.Orchestration.Outbox
  alias BorsNG.GitHub

  # Deployment alone never enables speculation. Operators must supply the
  # verified B/E/F evidence URLs as well as opt in to the small canary.
  def enabled?(project, branch) do
    System.get_env("BORS_SPECULATIVE_TRAINS", "false") == "true" and Outbox.enabled?() and
      project.name == "TauCetiProject/TauCeti" and branch == "main" and
      System.get_env("BORS_STAGE_DISPATCH_PROJECT") == project.name and
      Enum.all?([16231, 16232, 16234], fn issue ->
        value = System.get_env("BORS_SPECULATION_GATE_#{issue}", "")

        Regex.match?(
          ~r{\Ahttps://github.com/TauCetiProject/TauCeti/issues/#{issue}#issuecomment-[0-9]+\z},
          value
        )
      end)
  end

  def tracked?(nil), do: false
  def tracked?(candidate), do: event?(candidate, "train_reserved")
  def ref(candidate), do: "staging-candidate-#{candidate.id}-#{candidate.generation}"

  def event?(candidate, kind) do
    Repo.exists?(
      from(e in CandidateEvent, where: e.candidate_id == ^candidate.id and e.kind == ^kind)
    )
  end

  defp valid?(candidate) do
    binding = CandidateJournal.commit_binding(candidate)

    result =
      CandidateAssembler.identity(candidate)
      |> Map.merge(%{commit: binding["commit"], tree: binding["tree"]})

    CandidateAssembler.validate_result(candidate, result) == :ok and
      not event?(candidate, "train_invalidated")
  end

  def budget_available?(project_id) do
    cap =
      case Integer.parse(System.get_env("BORS_SPECULATION_CANARY_SUCCESSORS", "1")) do
        {n, ""} -> min(max(n, 1), 10)
        _ -> 1
      end

    used =
      Repo.aggregate(
        from(e in CandidateEvent,
          join: c in Candidate,
          on: c.id == e.candidate_id,
          where: c.project_id == ^project_id and e.kind == "successor_reserved"
        ),
        :count
      )

    used < cap
  end

  # Reservations include invalidated/withdrawn jobs until GitHub confirms that
  # their exact workflow has stopped. Database terminal state cannot free cost.
  def reservations(project_id) do
    Repo.all(
      from(c in Candidate,
        join: e in CandidateEvent,
        on: e.candidate_id == c.id and e.kind == "train_reserved",
        left_join: released in CandidateEvent,
        on: released.candidate_id == c.id and released.kind == "train_released",
        where: c.project_id == ^project_id and is_nil(released.id),
        order_by: c.id,
        select: c
      )
    )
  end

  def reserve(candidate, conn) do
    reconcile(candidate.project_id, conn)

    Repo.transaction(fn ->
      Repo.one!(from(p in Project, where: p.id == ^candidate.project_id, lock: "FOR UPDATE"))

      cond do
        tracked?(candidate) -> :ok
        length(reservations(candidate.project_id)) >= 3 -> Repo.rollback(:staging_cap)
        true -> CandidateJournal.event(candidate, "train_reserved", "bors.train", %{cap: 3})
      end
    end)
  end

  def reconcile(project_id, conn) do
    Enum.each(reservations(project_id), fn candidate ->
      batch = Repo.get(Batch, candidate.batch_id)
      requested = event?(candidate, "dispatch_requested")

      stopped =
        is_nil(batch) or batch.state not in [:running, :waiting] or
          CandidateJournal.latest(candidate.batch_id).id != candidate.id or
          event?(candidate, "construction_stopped")

      cond do
        stopped and not requested ->
          release(candidate, "never_dispatched")

        requested ->
          case run(candidate, conn) do
            {:ok, %{"status" => "completed"}} -> release(candidate, "workflow_completed")
            _ -> :ok
          end

        true ->
          :ok
      end
    end)
  end

  defp release(candidate, reason),
    do:
      CandidateJournal.event(candidate, "train_released", "github.train_reconcile", %{
        reason: reason
      })

  def run(candidate, conn) do
    binding = CandidateJournal.commit_binding(candidate)

    GitHub.staging_run(
      conn,
      binding["commit"],
      candidate.snapshot["base_sha"],
      candidate.batch_id,
      candidate.into_branch,
      candidate.observed_at
    )
  end

  # One successor per generation bounds wasted work even if a failing root
  # repeatedly receives green/red/stale notifications. Bundles remain in B's
  # existing atomic queue batch; predecessor members are never cloned into it.
  def predecessor(project, batches, conn) do
    running = Enum.filter(batches, &(&1.state == :running))

    with [parent] <- running,
         true <- enabled?(project, parent.into_branch),
         true <- budget_available?(project.id),
         candidate when not is_nil(candidate) <- CandidateJournal.current(parent),
         true <- tracked?(candidate),
         true <- valid?(candidate),
         false <- event?(candidate, "successor_reserved"),
         false <- Outbox.pending?(project.id, "dispatch:#{parent.id}"),
         %{commit: main} <- GitHub.get_branch!(conn, parent.into_branch),
         true <- main == parent.base_sha,
         true <- barrier_free?(candidate, conn),
         {:ok, :ok} <- reserve(candidate, conn),
         true <- length(reservations(project.id)) < 3 do
      candidate
    else
      _ -> nil
    end
  end

  def barrier_free?(candidate, conn) do
    binding = CandidateJournal.commit_binding(candidate)

    with hash when is_binary(hash) <- get_in(binding, ["toolchain", "sha256"]),
         true <- immutable_barriers_free?(candidate, binding),
         {:ok, content} when is_binary(content) <-
           GitHub.get_file(conn, candidate.snapshot["base_sha"], "lean-toolchain"),
         true <- CandidateJournal.digest(content) == hash,
         {:ok, toml} <- GetBorsToml.get_required(conn, candidate.snapshot["base_sha"]),
         true <-
           CandidateJournal.digest(Map.from_struct(toml)) == get_in(binding, ["policy", "sha256"]) do
      links_safe?(Repo.all(LinkPatchBatch.from_batch(candidate.batch_id)), conn)
    else
      _ -> false
    end
  end

  defp immutable_barriers_free?(candidate, binding) do
    event =
      Repo.one(
        from(e in CandidateEvent,
          where:
            e.candidate_id == ^candidate.id and e.kind == "train_barrier_checked" and
              e.source == "bors.local_git",
          order_by: [desc: e.id],
          limit: 1
        )
      )

    if event do
      details = event.details
      evidence = details["evidence"] || %{}
      paths = evidence["paths"]

      details["commit"] == binding["commit"] and details["tree"] == binding["tree"] and
        details["base_sha"] == candidate.snapshot["base_sha"] and evidence["complete"] == true and
        is_list(paths) and length(paths) < 300 and
        Enum.all?(paths, &(is_binary(&1) and not barrier_path?(&1)))
    else
      false
    end
  end

  def links_safe?(links, conn) do
    links != [] and length(links) <= 64 and
      Enum.all?(links, fn link ->
        with {:ok, pr} <- GitHub.get_pr(conn, link.patch.pr_xref),
             true <- pr.head_sha == link.head_sha and pr.state == :open and not pr.draft,
             {:ok, files} <- GitHub.get_pr_files(conn, link.patch.pr_xref),
             true <- is_list(files) and files != [] and length(files) < 3000 do
          Enum.all?(files, fn file ->
            paths =
              [Map.get(file, :filename), Map.get(file, :previous_filename)]
              |> Enum.reject(&is_nil/1)

            paths != [] and Enum.all?(paths, &(is_binary(&1) and not barrier_path?(&1)))
          end)
        else
          _ -> false
        end
      end)
  end

  def barrier_path?(path) do
    path in [
      "lean-toolchain",
      "lakefile.lean",
      "lakefile.toml",
      "lake-manifest.json",
      "bors.toml"
    ] or
      String.starts_with?(path, ["scripts/", ".github/"])
  end

  # Called before dispatch and before handling a result, even after disabling
  # the flag or restarting. Exact base/head and predecessor generation endure.
  def guard(batch, conn) do
    candidate = CandidateJournal.latest(batch.id)

    if tracked?(candidate) or (candidate && candidate.snapshot["speculative_predecessor_id"]) do
      cond do
        not valid?(candidate) or
            CandidateJournal.commit_binding(candidate)["commit"] != batch.commit ->
          {:invalidate, :generation_changed}

        Outbox.withdrawal_pending?(batch.project_id, member_ids(candidate)) ->
          {:hold, :withdrawal_pending}

        true ->
          predecessor_guard(batch, candidate, conn)
      end
    else
      :ok
    end
  end

  defp predecessor_guard(batch, candidate, conn) do
    main = GitHub.get_branch!(conn, batch.into_branch).commit

    case candidate.snapshot["speculative_predecessor_id"] do
      nil ->
        if main == batch.base_sha or
             (main == batch.commit and event?(candidate, "promotion_requested")),
           do: :ok,
           else: {:invalidate, :main_advanced}

      id ->
        parent = Repo.get!(Candidate, id)
        parent_batch = Repo.get(Batch, parent.batch_id)
        parent_head = CandidateJournal.commit_binding(parent)["commit"]

        cond do
          batch.base_sha != parent_head ->
            {:invalidate, :base_mismatch}

          parent_batch && parent_batch.state == :ok and event?(parent, "promoted") and
            parent_batch.commit == parent_head and
              CandidateJournal.latest(parent.batch_id).id == id ->
            if main == parent_head or
                 (main == batch.commit and event?(candidate, "promotion_requested")),
               do: :ok,
               else: {:invalidate, :main_advanced}

          is_nil(parent_batch) or parent_batch.state != :running or not valid?(parent) ->
            {:invalidate, :predecessor_invalid}

          main != parent_batch.base_sha ->
            {:invalidate, :main_advanced}

          Outbox.withdrawal_pending?(batch.project_id, member_ids(parent)) ->
            {:hold, :withdrawal_pending}

          true ->
            {:hold, :predecessor_running}
        end
    end
  end

  def member_ids(candidate), do: Enum.map(candidate.snapshot["members"], & &1["patch_id"])

  def invalidate(batch, reason) do
    # Retain exact-head approvals by cloning only this batch's own members.
    # Old generation/statuses remain terminal history. Replayed invalidation
    # finds the already committed retry through Divider's durable ancestry.
    {:ok, _} =
      Repo.transaction(fn ->
        current = Repo.one!(from(b in Batch, where: b.id == ^batch.id, lock: "FOR UPDATE"))

        if current.state == :running do
          candidate = CandidateJournal.current(current)
          links = Repo.all(LinkPatchBatch.from_batch(current.id))
          Divider.clone_batch(links, current.project_id, current.into_branch, "train_invalidated")
          CandidateJournal.event(candidate, "train_invalidated", "bors.train", %{reason: reason})
          current |> Batch.changeset(%{state: :canceled, commit: nil}) |> Repo.update!()
        end
      end)

    invalidate_descendants(batch)
    :canceled
  end

  def invalidate_descendants(batch) do
    if parent = CandidateJournal.latest(batch.id) do
      Repo.all(Batch.all_for_project(batch.project_id, :running))
      |> Enum.each(fn child ->
        candidate = CandidateJournal.current(child)

        if candidate && candidate.snapshot["speculative_predecessor_id"] == parent.id,
          do: invalidate(child, :predecessor_invalid)
      end)
    end
  end
end
