defmodule BorsNG.Worker.CandidateJournal do
  @moduledoc """
  Candidate assembly contract shared by independent assemblers. This journal
  records evidence; it neither admits work nor grants merge/cache authority.
  All writes append. See CANDIDATES.md for unknowns and timestamp semantics.
  """
  import Ecto.Query
  alias BorsNG.Database.{Batch, Candidate, CandidateAdmission, CandidateEvent, Patch, Repo}

  def insert_member(changeset, source_link \\ nil, retry_reason \\ "retry") do
    {:ok, link} =
      Repo.transaction(fn ->
        changeset |> Repo.insert!() |> admit(source_link, retry_reason)
      end)

    link
  end

  def admit(link, source_link \\ nil, retry_reason \\ "retry") do
    batch = Repo.get!(Batch, link.batch_id)
    patch = Repo.get!(Patch, link.patch_id)
    parent = if source_link, do: latest(source_link.batch_id)
    original = if source_link, do: admission(source_link.id)
    observed_at = now()

    Repo.insert!(%CandidateAdmission{
      project_id: batch.project_id,
      into_branch: batch.into_branch,
      batch_id: batch.id,
      link_id: link.id,
      observed_at: observed_at,
      snapshot: %{
        "pr" => patch.pr_xref,
        "base" => batch.into_branch,
        "patch_id" => patch.id,
        "head_sha" => link.head_sha,
        "reviewer" => link.reviewer,
        "manual_approval" => link.manual_approval,
        "bundle_id" => patch.bundle_id,
        "source" => if(source_link, do: "bors.retry", else: "bors.admission"),
        "retry_reason" => if(source_link, do: retry_reason),
        "parent_batch_id" => if(source_link, do: source_link.batch_id),
        "parent_candidate_id" => if(parent, do: parent.id),
        "parent_admission_id" => if(original, do: original.id),
        "original_admitted_at" =>
          cond do
            original -> original.snapshot["original_admitted_at"]
            source_link -> nil
            true -> observed_at
          end
      }
    })

    link
  end

  def admission(link_id), do: Repo.get_by(CandidateAdmission, link_id: link_id)

  def latest(batch_id) do
    Repo.one(
      from(c in Candidate,
        where: c.batch_id == ^batch_id,
        order_by: [desc: c.generation],
        limit: 1
      )
    )
  end

  # Call under normal scheduler authorization, with links in actual merge order.
  # A new generation is allocated on every construction attempt, including restart.
  def begin(batch, links, base_sha, predecessor_id \\ nil) do
    {:ok, candidate} =
      Repo.transaction(fn ->
        Repo.one!(from(b in Batch, where: b.id == ^batch.id, lock: "FOR UPDATE"))
        previous = latest(batch.id)
        predecessor = if predecessor_id, do: Repo.get!(Candidate, predecessor_id)

        if predecessor &&
             (predecessor.project_id != batch.project_id or
                predecessor.snapshot["base"] != batch.into_branch or
                commit_binding(predecessor)["commit"] != base_sha),
           do:
             raise(ArgumentError, "predecessor must bind this repository, branch and exact base")

        members =
          Enum.map(links, fn link ->
            case admission(link.id) do
              nil ->
                %{
                  "pr" => link.patch.pr_xref,
                  "patch_id" => link.patch_id,
                  "head_sha" => link.head_sha,
                  "reviewer" => link.reviewer,
                  "manual_approval" => link.manual_approval,
                  "bundle_id" => nil,
                  "admission_id" => nil,
                  "original_admitted_at" => nil,
                  "parent_batch_id" => nil,
                  "parent_candidate_id" => nil,
                  "source" => "legacy.unknown"
                }

              a ->
                Map.put(a.snapshot, "admission_id", a.id)
            end
            |> Map.put("assembly_head_sha", link.patch.commit)
          end)

        candidate =
          Repo.insert!(%Candidate{
            project_id: batch.project_id,
            into_branch: batch.into_branch,
            batch_id: batch.id,
            generation: if(previous, do: previous.generation + 1, else: 1),
            observed_at: now(),
            snapshot: %{
              "contract" => "tauceti-bors.candidate/v1",
              "base" => batch.into_branch,
              "base_sha" => base_sha,
              "members" => members,
              "previous_generation_id" => if(previous, do: previous.id),
              "speculative_predecessor_id" => predecessor_id
            }
          })

        event(candidate, "construction_started", "bors.assembly")
        candidate
      end)

    candidate
  end

  def event(candidate, kind, source, details \\ %{})
  def event(nil, _, _, _), do: :ok

  def event(candidate, kind, source, details) do
    Repo.insert!(%CandidateEvent{
      candidate_id: candidate.id,
      kind: kind,
      source: source,
      details: json_map(details),
      observed_at: now()
    })

    :ok
  end

  def bind(candidate, commit, tree, policy_ref, toml, toolchain) do
    # Rebinding would destroy the identity of a generation. A restart begins
    # a new generation instead, even when GitHub returns the same commit SHA.
    {:ok, :ok} =
      Repo.transaction(fn ->
        Repo.one!(from(c in Candidate, where: c.id == ^candidate.id, lock: "FOR UPDATE"))
        if commit_binding(candidate) != %{}, do: raise(ArgumentError, "candidate already bound")

        result =
          BorsNG.Worker.CandidateAssembler.identity(candidate)
          |> Map.merge(%{commit: commit, tree: tree})

        case BorsNG.Worker.CandidateAssembler.validate_result(candidate, result) do
          :ok -> :ok
          {:error, reason} -> raise(ArgumentError, "assembly result rejected: #{reason}")
        end

        event(candidate, "constructed", "github.commit_response", %{
          commit: commit,
          tree: tree,
          assembly_identity: BorsNG.Worker.CandidateAssembler.identity(candidate),
          base_sha: candidate.snapshot["base_sha"],
          policy: %{
            ref: policy_ref,
            effective: Map.from_struct(toml),
            sha256: digest(Map.from_struct(toml)),
            digest_encoding: "canonical_json/sorted_keys"
          },
          toolchain: toolchain
        })
      end)

    :ok
  end

  def commit_binding(candidate) do
    case Repo.one(
           from(e in CandidateEvent,
             where: e.candidate_id == ^candidate.id and e.kind == "constructed",
             order_by: e.id,
             limit: 1
           )
         ) do
      nil -> %{}
      event -> event.details
    end
  end

  # Only the current generation and its exact commit may receive lifecycle facts.
  # Old terminal generations retain their evidence when Batch.commit is cleared.
  def current(batch) do
    case latest(batch.id) do
      nil ->
        nil

      candidate ->
        if commit_binding(candidate)["commit"] == batch.commit and is_binary(batch.commit),
          do: candidate
    end
  end

  def status(batch, identifier, state, source, url \\ nil) do
    event(current(batch), "status_observed", source, %{
      commit: batch.commit,
      identifier: identifier,
      state: state,
      url: url,
      generation_binding: "commit_only",
      runner_lifecycle_authority: false
    })
  end

  def diagnostics(project_id, base, batch_id \\ nil) do
    query = from(c in Candidate, where: c.project_id == ^project_id, order_by: [desc: c.id])
    query = if base, do: from(c in query, where: c.into_branch == ^base), else: query

    query =
      if batch_id,
        do: from(c in query, where: c.batch_id == ^batch_id),
        else: from(c in query, limit: 100)

    candidates = Repo.all(query)
    ids = Enum.map(candidates, & &1.id)

    events =
      Repo.all(from(e in CandidateEvent, where: e.candidate_id in ^ids, order_by: e.id))
      |> Enum.group_by(& &1.candidate_id)

    Enum.map(candidates, fn c ->
      %{
        id: c.id,
        batch_id: c.batch_id,
        generation: c.generation,
        observed_at: c.observed_at,
        snapshot: c.snapshot,
        events:
          Enum.map(
            events[c.id] || [],
            &Map.take(&1, [:id, :kind, :source, :observed_at, :details])
          ),
        runner_assigned_at: nil,
        runner_started_at: nil,
        runner_finished_at: nil,
        unknown_reason: "runner lifecycle is not supplied by commit statuses"
      }
    end)
  end

  def digest(value) when is_binary(value),
    do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)

  def digest(value),
    do:
      :crypto.hash(:sha256, value |> json_map() |> canonical_json())
      |> Base.encode16(case: :lower)

  defp canonical_json(value) when is_map(value) do
    fields =
      value
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {key, val} -> Jason.encode!(key) <> ":" <> canonical_json(val) end)

    "{" <> Enum.join(fields, ",") <> "}"
  end

  defp canonical_json(value) when is_list(value),
    do: "[" <> Enum.map_join(value, ",", &canonical_json/1) <> "]"

  defp canonical_json(value), do: Jason.encode!(value)

  defp json_map(value), do: value |> Jason.encode!() |> Jason.decode!()
  defp now, do: System.system_time(:microsecond)
end
