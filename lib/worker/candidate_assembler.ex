defmodule BorsNG.Worker.CandidateAssembler do
  @moduledoc """
  Assembly boundary for J (local Git) and K (asynchronous orchestration).

  An assembler consumes immutable ordered inputs and returns an exact commit
  and tree, or structured conflict/failure evidence. It cannot promote, dispatch
  CI, authorize reviews or publish caches. Context supplies repository access,
  policy and attribution metadata; implementations must not substitute live
  heads. External mutations and their recovery belong to the authorized caller.
  The serial GitHub path records the same result envelope at its existing
  combined create/push boundary; see CANDIDATES.md.
  """
  alias BorsNG.Database.{Candidate, LinkPatchBatch, Repo}
  alias BorsNG.Worker.CandidateJournal

  @type input :: Candidate.t()
  @type result :: %{
          candidate_id: pos_integer(),
          generation: pos_integer(),
          base_sha: String.t(),
          members_digest: String.t(),
          commit: String.t(),
          tree: String.t()
        }
  @callback assemble(input(), map()) ::
              {:ok, result()} | {:conflict, map()} | {:error, map()}

  def identity(candidate) do
    %{
      candidate_id: candidate.id,
      generation: candidate.generation,
      base_sha: candidate.snapshot["base_sha"],
      members_digest: CandidateJournal.digest(candidate.snapshot["members"])
    }
  end

  @doc "Validate result identity before recording or acting on an async result."
  def validate_result(candidate, result) do
    latest = CandidateJournal.latest(candidate.batch_id)
    batch = Repo.get(BorsNG.Database.Batch, candidate.batch_id)

    cond do
      Map.take(result, [:candidate_id, :generation, :base_sha, :members_digest]) !=
          identity(candidate) ->
        {:error, :identity_mismatch}

      is_nil(latest) or latest.id != candidate.id or is_nil(batch) or
          batch.state not in [:waiting, :running] ->
        {:error, :stale_candidate}

      not is_binary(result[:commit]) or not is_binary(result[:tree]) ->
        {:error, :incomplete_result}

      not members_current?(candidate) ->
        {:error, :membership_changed}

      true ->
        :ok
    end
  end

  defp members_current?(candidate) do
    fields = ~w(link_id patch_id head_sha reviewer manual_approval assembly_head_sha)
    expected = candidate.snapshot["members"] |> Enum.map(&Map.take(&1, fields))

    actual =
      Repo.all(LinkPatchBatch.from_batch(candidate.batch_id))
      |> Enum.map(fn link ->
        %{
          "link_id" => link.id,
          "patch_id" => link.patch_id,
          "head_sha" => link.head_sha,
          "reviewer" => link.reviewer,
          "manual_approval" => link.manual_approval,
          "assembly_head_sha" => link.patch.commit
        }
      end)

    Enum.sort_by(expected, & &1["link_id"]) == Enum.sort_by(actual, & &1["link_id"])
  end
end
