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
    handoff = if source_link, do: nil, else: readmission(batch, patch, link, observed_at)

    snapshot = %{
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
      "remainder_retries" =>
        if(source_link,
          do:
            BorsNG.Worker.Batcher.QueueAge.next_remainder(
              if(original, do: original.snapshot),
              retry_reason
            ),
          else: 0
        ),
      "queue_age_retries" =>
        cond do
          source_link ->
            BorsNG.Worker.Batcher.QueueAge.next_retry(
              if(original, do: original.snapshot),
              retry_reason
            )

          handoff ->
            handoff.retries

          true ->
            0
        end,
      "original_admitted_at" =>
        cond do
          original -> original.snapshot["original_admitted_at"]
          source_link -> nil
          handoff -> handoff.age
          true -> observed_at
        end,
      "scheduling" =>
        if(source_link,
          do: if(original, do: original.snapshot["scheduling"]),
          else: %{
            "priority" => patch.priority,
            "singleton" => patch.is_single,
            "label_hint" =>
              if(patch.commit == link.head_sha,
                do: BorsNG.Worker.Batcher.Diversity.cached(patch)
              ),
            "policy" => "existing_admission; diversity_disabled"
          }
        )
    }

    # Additive: ordinary admissions keep their exact previous shape.
    snapshot =
      if handoff,
        do:
          Map.put(snapshot, "inherited_handoff", %{
            "candidate_id" => handoff.candidate.id,
            "event_id" => handoff.event.id,
            "head_sha" => handoff.suspect["head_sha"],
            "admission_id" => handoff.suspect["admission_id"],
            "handed_off_at" => handoff.event.observed_at
          }),
        else: snapshot

    admission =
      Repo.insert!(%CandidateAdmission{
        project_id: batch.project_id,
        into_branch: batch.into_branch,
        batch_id: batch.id,
        link_id: link.id,
        observed_at: observed_at,
        snapshot: snapshot
      })

    # The consumption marker lives beside the handoff it consumes, so the next
    # fresh admission of this PR finds it in one indexed read and starts fresh.
    if handoff,
      do:
        event(handoff.candidate, "suspect_readmitted", "bors.admission", %{
          pr: patch.pr_xref,
          patch_id: patch.id,
          head_sha: link.head_sha,
          handoff_head_sha: handoff.suspect["head_sha"],
          handoff_event_id: handoff.event.id,
          admission_id: admission.id,
          link_id: link.id,
          batch_id: batch.id,
          original_admitted_at: handoff.age,
          queue_age_retries: handoff.retries,
          readmitted_at: observed_at
        })

    link
  end

  # The handoff's own record of each withdrawn suspect: the age its admission
  # carried (possibly already inherited through retries) and the allowance
  # left on it. Nothing is recorded for a missing or mismatched admission.
  def handoff_record(link) do
    case admission(link.id) do
      %{snapshot: snapshot} = a when is_map(snapshot) ->
        if snapshot["head_sha"] == link.head_sha and snapshot["patch_id"] == link.patch_id,
          do: %{
            "pr" => snapshot["pr"],
            "head_sha" => link.head_sha,
            "patch_id" => link.patch_id,
            "link_id" => link.id,
            "admission_id" => a.id,
            "original_admitted_at" => snapshot["original_admitted_at"],
            "queue_age_retries" => Map.get(snapshot, "queue_age_retries", 0)
          },
          else: unrecorded(link)

      _ ->
        unrecorded(link)
    end
  end

  defp unrecorded(link) do
    %{
      "pr" => nil,
      "head_sha" => link.head_sha,
      "patch_id" => link.patch_id,
      "link_id" => link.id,
      "admission_id" => nil,
      "original_admitted_at" => nil,
      "queue_age_retries" => nil
    }
  end

  # The latest repair handoff naming this PR, if its retained age is usable at
  # this head and no fresh admission has consumed it yet. One read at approval
  # time over this repository's `provisional_repair` events, never on the
  # scheduler path; the consumption check is bounded by that candidate's events.
  # A head whose repair request is still active inherits nothing: automatic
  # admission never offers it, and a human `r+` of it is ejected at assembly by
  # SourceCompatibility.admit, so that admission must not consume the grant.
  defp readmission(batch, patch, link, now) do
    found =
      Repo.one(
        from(e in CandidateEvent,
          join: c in Candidate,
          on: c.id == e.candidate_id,
          where:
            c.project_id == ^batch.project_id and c.into_branch == ^batch.into_branch and
              e.kind == "provisional_repair" and e.source == "bors.failure_isolation" and
              fragment("? @> ?::jsonb", e.details, ^%{"prs" => [patch.pr_xref]}),
          order_by: [desc: e.id],
          limit: 1,
          select: {c, e}
        )
      )

    with {candidate, event} <- found,
         false <- BorsNG.Worker.SourceCompatibility.requested?(patch, link.head_sha),
         suspects when is_list(suspects) <- event.details["suspects"],
         %{} = suspect <- Enum.find(suspects, &(is_map(&1) and &1["pr"] == patch.pr_xref)),
         {age, retries} <-
           BorsNG.Worker.Batcher.QueueAge.readmission(suspect, link.head_sha, now),
         false <- consumed?(candidate, event, patch) do
      %{candidate: candidate, event: event, suspect: suspect, age: age, retries: retries}
    else
      _ -> nil
    end
  end

  defp consumed?(candidate, event, patch) do
    Repo.exists?(
      from(e in CandidateEvent,
        where:
          e.candidate_id == ^candidate.id and e.id > ^event.id and
            e.kind == "suspect_readmitted" and
            fragment("? @> ?::jsonb", e.details, ^%{"pr" => patch.pr_xref})
      )
    )
  end

  def admission(link_id), do: Repo.get_by(CandidateAdmission, link_id: link_id)

  # Reads bounded by current waiting membership and its immediate split parents.
  # No historical scan, per-member lookup, JSON SQL or GitHub calls.
  def queue_age_members(batches) do
    ids = for b <- batches, b.state == :waiting, do: b.id

    if ids == [] do
      []
    else
      Repo.all(
        from(l in BorsNG.Database.LinkPatchBatch,
          join: p in Patch,
          on: p.id == l.patch_id,
          left_join: a in CandidateAdmission,
          on: a.link_id == l.id,
          where: l.batch_id in ^ids,
          select: {l, p.commit, a}
        )
      )
      |> annotate_legacy_remainders()
    end
  end

  # Older diagnostic retries used the same reason for suspect and remainder.
  # Recover the role only from an exact, two-part immutable partition. More
  # complex singleton/component layouts remain ambiguous. This is one bounded
  # database join for current waiting parents, never a scan or a GitHub read.
  defp annotate_legacy_remainders(rows) do
    reasons =
      ~w(compiler_diagnostics import_provider_hint bounded_provider_hint recovered_parent_diagnostics)

    parents =
      for {_, _, a} <- rows,
          a != nil,
          a.snapshot["retry_reason"] in reasons,
          is_integer(a.snapshot["parent_candidate_id"]),
          do: a.snapshot["parent_candidate_id"]

    parents = Enum.uniq(parents)

    observations =
      if parents == [],
        do: [],
        else:
          Repo.all(
            from(c in Candidate,
              join: e in CandidateEvent,
              on: e.candidate_id == c.id,
              where:
                c.id in ^parents and e.kind == "failure_partition" and
                  e.source == "bors.failure_isolation",
              order_by: [desc: e.id],
              select: {c, e}
            )
          )

    profiles =
      Enum.flat_map(observations, fn {c, e} ->
        members = Map.new(c.snapshot["members"], &{&1["pr"], &1["head_sha"]})

        case e.details["groups"] do
          [suspects, remainder] when suspects != [] and remainder != [] ->
            all = suspects ++ remainder

            if e.details["ownership_complete"] == true and e.details["authority"] == "retry_only" and
                 Enum.sort(all) == Enum.sort(Map.keys(members)) and
                 length(Enum.uniq(all)) == length(all),
               do: [{c, e, members, MapSet.new(remainder)}],
               else: []

          _ ->
            []
        end
      end)
      |> Enum.group_by(fn {c, _, _, _} -> c.id end)

    Enum.map(rows, fn {link, head, a} = row ->
      if a && a.snapshot["retry_reason"] in reasons do
        matched =
          Enum.any?(Map.get(profiles, a.snapshot["parent_candidate_id"], []), fn {c, e, members,
                                                                                  remainder} ->
            c.project_id == a.project_id and c.into_branch == a.into_branch and
              e.observed_at <= a.observed_at and
              e.details["strategy"] == a.snapshot["retry_reason"] and
              MapSet.member?(remainder, a.snapshot["pr"]) and members[a.snapshot["pr"]] == head and
              head == link.head_sha and head == a.snapshot["head_sha"]
          end)

        if matched,
          do: {link, head, %{a | snapshot: Map.put(a.snapshot, "verified_remainder", true)}},
          else: row
      else
        row
      end
    end)
  end

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
                # Admission is historical provenance, including the original
                # approval. A same-head human takeover updates the queued link;
                # freeze that decision in this generation without rewriting age,
                # reviewer, admitted head, scheduling or ancestry evidence.
                a.snapshot
                |> Map.put("admission_id", a.id)
                |> Map.put("manual_approval", link.manual_approval)
            end
            |> Map.put("link_id", link.id)
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

  def bind(
        candidate,
        commit,
        tree,
        policy_ref,
        toml,
        toolchain,
        source \\ "github.commit_response"
      ) do
    # Rebinding would destroy the identity of a generation. A restart begins
    # a new generation instead, even when GitHub returns the same commit SHA.
    {:ok, :ok} =
      Repo.transaction(fn ->
        # Serialize visibility with generation allocation and cancellation.
        Repo.one!(from(b in Batch, where: b.id == ^candidate.batch_id, lock: "FOR UPDATE"))
        Repo.one!(from(c in Candidate, where: c.id == ^candidate.id, lock: "FOR UPDATE"))
        if commit_binding(candidate) != %{}, do: raise(ArgumentError, "candidate already bound")

        result =
          BorsNG.Worker.CandidateAssembler.identity(candidate)
          |> Map.merge(%{commit: commit, tree: tree})

        case BorsNG.Worker.CandidateAssembler.validate_result(candidate, result) do
          :ok ->
            :ok

          {:error, reason} ->
            raise BorsNG.Worker.CandidateAssembler.Rejected,
              candidate: candidate,
              reason: reason
        end

        event(candidate, "constructed", source, %{
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

  @doc "Preserve GitHub check lifecycle timestamps without changing queue status."
  def observe_check(project_id, %{"head_sha" => commit} = check) when is_binary(commit) do
    Repo.all(Batch.get_assoc_by_commit(project_id, commit, :incomplete))
    |> Enum.each(fn batch ->
      event(current(batch), "check_run_observed", "github.check_run_webhook", %{
        commit: commit,
        check_run_id: check["id"],
        check_suite_id: get_in(check, ["check_suite", "id"]),
        app_id: get_in(check, ["app", "id"]),
        identifier: check["name"],
        status: check["status"],
        conclusion: check["conclusion"],
        url: check["html_url"],
        check_started_at: github_time(check["started_at"]),
        check_finished_at: github_time(check["completed_at"]),
        timestamp_source: "github.check_run",
        generation_binding: "commit_only",
        runner_lifecycle_authority: false
      })
    end)

    :ok
  end

  def observe_check(_, _), do: :ok

  # Reporting needs complete promotion history, not the rolling diagnostic
  # window or the much larger assembly/admission provenance stored beside it.
  def promotions(project_id, base, batch_id \\ nil) do
    promoted = from(e in CandidateEvent, where: e.kind == "promoted", select: e.candidate_id)

    query =
      from(c in Candidate,
        where: c.project_id == ^project_id and c.id in subquery(promoted),
        order_by: c.id
      )

    query = if base, do: from(c in query, where: c.into_branch == ^base), else: query
    query = if batch_id, do: from(c in query, where: c.batch_id == ^batch_id), else: query

    events =
      Repo.all(
        from(e in CandidateEvent,
          where:
            e.candidate_id in subquery(from(c in query, select: c.id)) and
              e.kind in ["constructed", "promoted"],
          order_by: e.id
        )
      )
      |> Enum.group_by(& &1.candidate_id)

    Repo.all(query)
    |> Enum.map(fn c ->
      snapshot = Map.take(c.snapshot, ["contract", "base", "base_sha"])
      members = Enum.map(c.snapshot["members"] || [], &Map.take(&1, ["pr", "head_sha"]))

      %{
        id: c.id,
        batch_id: c.batch_id,
        snapshot: Map.put(snapshot, "members", members),
        events:
          Enum.map(events[c.id] || [], fn event ->
            event
            |> Map.take([:kind, :source, :observed_at])
            |> Map.put(:details, Map.take(event.details || %{}, ["base", "base_sha", "commit"]))
          end)
      }
    end)
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

  defp github_time(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, time, _} -> DateTime.to_unix(time, :microsecond)
      _ -> nil
    end
  end

  defp github_time(_), do: nil
  defp now, do: System.system_time(:microsecond)
end
