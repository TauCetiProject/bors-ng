defmodule BorsNG.Worker.CandidateCancellationTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    Candidate,
    CandidateEvent,
    Installation,
    OrchestrationJob,
    Project,
    Repo
  }

  alias BorsNG.Worker.{CandidateCancellation, CandidateJournal}
  alias BorsNG.Worker.Orchestration.Outbox
  alias BorsNG.GitHub.ServerMock

  import Phoenix.ConnTest
  @endpoint BorsNG.Endpoint

  @head String.duplicate("a", 40)
  @base String.duplicate("b", 40)
  @conn {{:installation, 91}, 1_256_881_326}

  setup do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    on_exit(fn -> :sys.resume(BorsNG.Worker.Orchestration.Dispatcher) end)

    old =
      for k <- ["BORS_ASYNC_ORCHESTRATION", "BORS_STAGE_DISPATCH_PROJECT"],
          into: %{},
          do: {k, System.get_env(k)}

    System.put_env("BORS_ASYNC_ORCHESTRATION", "false")
    System.put_env("BORS_STAGE_DISPATCH_PROJECT", "TauCetiProject/TauCeti")

    on_exit(fn ->
      Enum.each(old, fn {k, v} -> if v, do: System.put_env(k, v), else: System.delete_env(k) end)
    end)

    inst = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        repo_xref: 1_256_881_326,
        installation_id: inst.id
      })

    batch =
      Repo.insert!(%Batch{
        project_id: project.id,
        into_branch: "main",
        state: :canceled,
        commit: @head,
        base_sha: @base
      })

    candidate =
      Repo.insert!(%Candidate{
        project_id: project.id,
        batch_id: batch.id,
        generation: 1,
        into_branch: "main",
        observed_at: System.system_time(:microsecond),
        snapshot: %{"base_sha" => @base}
      })

    CandidateJournal.event(candidate, "constructed", "test", %{commit: @head, base_sha: @base})

    CandidateJournal.event(candidate, "dispatch_requested", "test", %{
      commit: @head,
      base_sha: @base
    })

    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    CandidateCancellation.enqueue(batch)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "false")
    job = Repo.get_by!(OrchestrationJob, kind: "cancel_run")
    assert job.priority == 0
    {:ok, p} = CandidateCancellation.authority(job.id)
    actor = %{"id" => 337_246_530, "login" => "tau-ceti-bors[bot]", "type" => "Bot"}

    run = %{
      "id" => 123,
      "run_attempt" => 1,
      "event" => "repository_dispatch",
      "path" => ".github/workflows/pr-build.yml",
      "workflow_id" => 288_851_159,
      "display_title" => CandidateCancellation.title(p),
      "actor" => actor,
      "triggering_actor" => actor,
      "repository" => %{"id" => 1_256_881_326, "full_name" => "TauCetiProject/TauCeti"},
      "head_sha" => @base,
      "status" => "in_progress",
      "conclusion" => nil,
      "created_at" => DateTime.utc_now() |> DateTime.to_iso8601()
    }

    ServerMock.put_state(%{@conn => %{staging_runs: %{{@head, batch.id} => run}}})
    {:ok, project: project, batch: batch, candidate: candidate, job: job, p: p, run: run}
  end

  defp claim(job) do
    {:ok, claimed} = Outbox.claim(job.id)
    assert claimed
    claimed
  end

  defp ready(job) do
    Repo.get!(OrchestrationJob, job.id)
    |> Ecto.Changeset.change(available_at: Outbox.now())
    |> Repo.update!()
  end

  defp unauthorized(ctx),
    do: assert({:error, :unauthorized_or_expired} == CandidateCancellation.authority(ctx.job.id))

  test "the real explicit-withdrawal handler persists the intent and retains authorized survivors",
       ctx do
    alias BorsNG.Database.{LinkPatchBatch, Patch}
    Repo.delete!(ctx.job)
    ctx.batch |> Batch.changeset(%{state: :running}) |> Repo.update!()

    patches =
      for number <- 1..2 do
        patch =
          Repo.insert!(%Patch{
            project_id: ctx.project.id,
            pr_xref: number,
            open: true,
            commit: @head,
            into_branch: "main",
            queue_pending: true,
            queue_approval: %{"state" => "approved", "head_sha" => @head, "reviewer" => "rvr"}
          })

        CandidateJournal.insert_member(
          LinkPatchBatch.changeset(
            %LinkPatchBatch{},
            %{batch_id: ctx.batch.id, patch_id: patch.id, head_sha: @head, reviewer: "rvr"}
          )
        )

        patch
      end

    [withdrawn, survivor] = patches
    state = ServerMock.get_state()

    state =
      update_in(
        state,
        [@conn],
        &Map.merge(&1, %{
          comments: %{1 => [], 2 => []},
          labels: %{1 => [], 2 => []},
          statuses: %{@head => %{}}
        })
      )

    ServerMock.put_state(state)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    BorsNG.Worker.Batcher.handle_cast({:cancel, withdrawn.id, :requested}, ctx.project.id)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "false")
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    intent = Repo.get_by!(OrchestrationJob, kind: "cancel_run")
    assert {:ok, _} = CandidateCancellation.authority(intent.id)
    [child] = Repo.all(Batch.all_for_project(ctx.project.id, :incomplete))
    [link] = Repo.all(LinkPatchBatch.from_batch(child.id))
    assert link.patch_id == survivor.id and link.head_sha == @head and link.reviewer == "rvr"
    assert Repo.get!(Patch, survivor.id).queue_approval == survivor.queue_approval
    refute ServerMock.get_state()[:cancellations]
  end

  test "real queue-wide cancellation binds running work once and never cancels waiting work",
       ctx do
    alias BorsNG.Database.{LinkPatchBatch, Patch}
    Repo.delete!(ctx.job)
    ctx.batch |> Batch.changeset(%{state: :running}) |> Repo.update!()
    waiting = Repo.insert!(Batch.new(ctx.project.id, "main", 0))

    for {batch, number} <- [{ctx.batch, 1}, {waiting, 2}] do
      patch =
        Repo.insert!(%Patch{
          project_id: ctx.project.id,
          pr_xref: number,
          open: true,
          commit: @head,
          into_branch: "main",
          queue_pending: true,
          queue_approval: %{"state" => "approved", "head_sha" => @head, "reviewer" => "rvr"}
        })

      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(
          %LinkPatchBatch{},
          %{batch_id: batch.id, patch_id: patch.id, head_sha: @head, reviewer: "rvr"}
        )
      )
    end

    state =
      update_in(
        ServerMock.get_state(),
        [@conn],
        &Map.merge(
          &1,
          %{comments: %{1 => [], 2 => []}, labels: %{1 => [], 2 => []}, statuses: %{@head => %{}}}
        )
      )

    ServerMock.put_state(state)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    BorsNG.Worker.Batcher.handle_cast({:cancel_all}, ctx.project.id)
    BorsNG.Worker.Batcher.handle_cast({:cancel_all}, ctx.project.id)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "false")
    assert Repo.get!(Batch, ctx.batch.id).state == :canceled
    refute Repo.get(Batch, waiting.id)
    assert Repo.all(Batch.all_for_project(ctx.project.id, :incomplete)) == []
    [intent] = Repo.all(from(j in OrchestrationJob, where: j.kind == "cancel_run"))

    assert {:ok, %{"batch_id" => batch_id, "candidate_id" => candidate_id}} =
             CandidateCancellation.authority(intent.id)

    assert batch_id == ctx.batch.id and candidate_id == ctx.candidate.id

    assert Enum.all?(
             Repo.all(from(p in Patch, where: p.project_id == ^ctx.project.id)),
             &(not &1.queue_pending and &1.queue_approval["state"] != "approved")
           )

    refute ServerMock.get_state()[:cancellations]
  end

  test "public authority is read-only, uncached and revokes on terminal job", ctx do
    conn = get(build_conn(), "/api/candidate-cancellations/#{ctx.job.id}")
    assert conn.status == 200
    assert Plug.Conn.get_resp_header(conn, "cache-control") == ["no-store"]
    assert Jason.decode!(conn.resp_body)["generation"] == 1
    ctx.job |> Ecto.Changeset.change(state: "done") |> Repo.update!()
    assert get(build_conn(), "/api/candidate-cancellations/#{ctx.job.id}").status == 404
    assert get(build_conn(), "/api/candidate-cancellations/bad").status == 404
  end

  test "enqueue and withdrawal state commit atomically and never contact GitHub", ctx do
    Repo.delete!(ctx.job)
    running = ctx.batch |> Batch.changeset(%{state: :running}) |> Repo.update!()
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")

    assert {:error, :test_rollback} =
             Repo.transaction(fn ->
               running |> Batch.changeset(%{state: :canceled}) |> Repo.update!()
               CandidateCancellation.enqueue(running)
               Repo.rollback(:test_rollback)
             end)

    System.put_env("BORS_ASYNC_ORCHESTRATION", "false")
    assert Repo.get!(Batch, ctx.batch.id).state == :running
    refute Repo.get_by(OrchestrationJob, kind: "cancel_run")
    refute ServerMock.get_state()[:cancellations]
  end

  test "real adapter refuses stale authority before beginning HTTP", ctx do
    assert {:error, :unauthorized_or_expired} =
             BorsNG.GitHub.Server.do_handle_call(
               :cancellation_dispatch,
               {{:raw, "test-token"}, 1_256_881_326},
               {ctx.job.id, 123, 1}
             )

    assert {:error, :unauthorized_or_expired} =
             BorsNG.GitHub.Server.do_handle_call(
               :cancellation_dispatch,
               {{:raw, "test-token"}, 1_256_881_326},
               {999_999, 123, 1}
             )
  end

  test "expired leases survive restart and still honor the finite attempt budget", ctx do
    job = claim(ctx.job)
    job |> Ecto.Changeset.change(lease_until: Outbox.now() - 1) |> Repo.update!()
    assert {1, nil} == Outbox.recover_expired()

    assert catch_throw(CandidateCancellation.deliver(claim(job))) ==
             {:cancellation_retry, :awaiting_ci_terminal}

    Repo.get!(OrchestrationJob, job.id) |> Ecto.Changeset.change(attempts: 7) |> Repo.update!()
    unauthorized(ctx)
  end

  test "exact bound dispatch uses workflow SHA without confusing it with candidate SHA", ctx do
    assert {:ok, ctx.run} == CandidateCancellation.validate_run(ctx.run, ctx.p)
    assert {:ok, _} = CandidateCancellation.authority(ctx.job.id)
  end

  test "distinct runs, generations, branches, actors, workflow and ordinary PR CI fail closed",
       ctx do
    for run <- [
          Map.put(ctx.run, "event", "pull_request_target"),
          Map.put(ctx.run, "workflow_id", 1),
          Map.put(ctx.run, "path", ".github/workflows/other.yml"),
          Map.put(ctx.run, "run_attempt", 2),
          Map.delete(ctx.run, "run_attempt"),
          put_in(ctx.run, ["actor", "id"], 1),
          put_in(ctx.run, ["triggering_actor", "id"], 1),
          put_in(ctx.run, ["repository", "id"], 1),
          Map.put(ctx.run, "created_at", "bad"),
          Map.put(
            ctx.run,
            "display_title",
            String.replace(CandidateCancellation.title(ctx.p), "branch=main", "branch=other")
          ),
          Map.put(
            ctx.run,
            "display_title",
            String.replace(CandidateCancellation.title(ctx.p), @base, @head)
          ),
          Map.put(
            ctx.run,
            "display_title",
            "bors branch=main batch=999 head=#{@head} base=#{@base}"
          )
        ] do
      assert {:error, :invalid_run_binding} == CandidateCancellation.validate_run(run, ctx.p)
    end
  end

  test "normal request is durable, replay idempotent and completed work stops delivery", ctx do
    job = claim(ctx.job)

    assert catch_throw(CandidateCancellation.deliver(job)) ==
             {:cancellation_retry, :awaiting_ci_terminal}

    assert [{id, 123, 1}] = ServerMock.get_state().cancellations
    assert id == ctx.job.id
    assert {:ok, %{"run_id" => 123, "run_attempt" => 1}} = CandidateCancellation.authority(id)
    Outbox.fail(job, :awaiting_ci_terminal)
    assert Repo.get!(OrchestrationJob, id).available_at >= Outbox.now() + 59
    ready(job)
    replay = claim(job)

    assert catch_throw(CandidateCancellation.deliver(replay)) ==
             {:cancellation_retry, :awaiting_ci_terminal}

    assert length(ServerMock.get_state().cancellations) == 1

    assert Repo.aggregate(
             from(e in CandidateEvent, where: e.kind == "cancellation_run_bound"),
             :count
           ) == 1

    state = ServerMock.get_state()

    ServerMock.put_state(
      put_in(
        state,
        [@conn, :staging_runs, {@head, ctx.batch.id}],
        Map.merge(ctx.run, %{"status" => "completed", "conclusion" => "cancelled"})
      )
    )

    assert :ok = CandidateCancellation.deliver(replay)
    Outbox.finish(replay)
    unauthorized(ctx)
    # Permanent deduplication survives completion/restart.
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    CandidateCancellation.enqueue(ctx.batch)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "false")
    assert Repo.aggregate(from(j in OrchestrationJob, where: j.kind == "cancel_run"), :count) == 1
  end

  test "discovery race and API errors back off durably and exhaust after six attempts", ctx do
    ServerMock.put_state(%{@conn => %{}})
    job = claim(ctx.job)

    assert catch_throw(CandidateCancellation.deliver(job)) ==
             {:cancellation_retry, {:retry, :run_not_discovered}}

    Outbox.fail(job, :run_not_discovered)
    assert Repo.get!(OrchestrationJob, job.id).state == "pending"
    ServerMock.put_state(%{cancellation_lookup_error: {:error, :dispatch_lookup, 503}})
    ready(job)
    job = claim(job)

    assert catch_throw(CandidateCancellation.deliver(job)) ==
             {:cancellation_retry, {:error, :dispatch_lookup, 503}}

    assert :ok == CandidateCancellation.deliver(%{job | token: "stale"})
    Outbox.fail(%{job | attempts: 6}, :api_unavailable)
    done = Repo.get!(OrchestrationJob, job.id)
    assert done.state == "done" and done.last_error =~ "cancellation_exhausted"
    unauthorized(ctx)
  end

  test "unlanded or changed companion capability never activates dispatch", ctx do
    for error <- [
          {:error, :cancellation_sink_not_landed},
          {:error, :cancellation_capability_mismatch}
        ] do
      ServerMock.put_state(Map.put(ServerMock.get_state(), :cancellation_capability_error, error))

      job =
        if Repo.get!(OrchestrationJob, ctx.job.id).state == "running",
          do: Repo.get!(OrchestrationJob, ctx.job.id),
          else: claim(ctx.job)

      assert catch_throw(CandidateCancellation.deliver(job)) == {:cancellation_retry, error}
      refute ServerMock.get_state()[:cancellations]
    end
  end

  test "ambiguous API acceptance permits only one replay and never amplifies every retry", ctx do
    ServerMock.put_state(
      Map.put(ServerMock.get_state(), :cancellation_dispatch_error, {:error, :dispatch, 503})
    )

    job = claim(ctx.job)

    for _ <- 1..2 do
      assert catch_throw(CandidateCancellation.deliver(job)) ==
               {:cancellation_retry, {:error, :dispatch, 503}}
    end

    assert catch_throw(CandidateCancellation.deliver(job)) ==
             {:cancellation_retry, :ambiguous_delivery_limit}

    assert length(ServerMock.get_state().cancellations) == 2
  end

  test "a run appearing after first lookup is discovered on retry", ctx do
    ServerMock.put_state(%{@conn => %{}})
    job = claim(ctx.job)

    assert catch_throw(CandidateCancellation.deliver(job)) ==
             {:cancellation_retry, {:retry, :run_not_discovered}}

    Outbox.fail(job, :run_not_discovered)
    ServerMock.put_state(%{@conn => %{staging_runs: %{{@head, ctx.batch.id} => ctx.run}}})
    ready(job)

    assert catch_throw(CandidateCancellation.deliver(claim(job))) ==
             {:cancellation_retry, :awaiting_ci_terminal}
  end

  test "expired authority, later requeue and protected promotion cannot cancel", ctx do
    for state <- [:waiting, :running, :ok] do
      Repo.get!(Batch, ctx.batch.id) |> Batch.changeset(%{state: state}) |> Repo.update!()
      unauthorized(ctx)
    end

    Repo.get!(Batch, ctx.batch.id) |> Batch.changeset(%{state: :canceled}) |> Repo.update!()

    for kind <- ["promotion_requested", "promoted"] do
      CandidateJournal.event(ctx.candidate, kind, "test")
      unauthorized(ctx)
      Repo.delete_all(from(e in CandidateEvent, where: e.kind == ^kind))
    end

    p = Map.put(Outbox.payload(ctx.job), "expires_at", Outbox.now() - 1)
    ctx.job |> Ecto.Changeset.change(payload: :erlang.term_to_binary(p)) |> Repo.update!()
    unauthorized(ctx)
  end

  test "new generation and indistinguishable generation identities never reuse authority", ctx do
    new =
      Repo.insert!(%Candidate{
        project_id: ctx.project.id,
        batch_id: ctx.batch.id,
        generation: 2,
        into_branch: "main",
        snapshot: %{"base_sha" => @base},
        observed_at: System.system_time(:microsecond)
      })

    CandidateJournal.event(new, "constructed", "test", %{commit: @head, base_sha: @base})
    unauthorized(ctx)
    # Even a new intent cannot identify which generation an identical title meant.
    CandidateJournal.event(new, "dispatch_requested", "test", %{commit: @head, base_sha: @base})
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")
    CandidateCancellation.enqueue(ctx.batch)
    System.put_env("BORS_ASYNC_ORCHESTRATION", "false")
    job = Repo.get_by!(OrchestrationJob, dedup_key: "cancel-run:#{new.id}")
    assert {:error, :unauthorized_or_expired} == CandidateCancellation.authority(job.id)
  end

  test "missing, duplicate and malformed dispatch evidence cannot authorize cancellation", ctx do
    CandidateJournal.event(ctx.candidate, "dispatch_requested", "test", %{
      commit: @head,
      base_sha: @base
    })

    unauthorized(ctx)
    Repo.delete_all(from(e in CandidateEvent, where: e.kind == "dispatch_requested"))
    unauthorized(ctx)
    CandidateJournal.event(ctx.candidate, "dispatch_requested", "test", %{})
    unauthorized(ctx)
  end

  test "a different run discovered on replay cannot replace the bound run", ctx do
    job = claim(ctx.job)

    assert catch_throw(CandidateCancellation.deliver(job)) ==
             {:cancellation_retry, :awaiting_ci_terminal}

    state = ServerMock.get_state()

    ServerMock.put_state(
      put_in(state, [@conn, :staging_runs, {@head, ctx.batch.id}], Map.put(ctx.run, "id", 456))
    )

    assert :ok == CandidateCancellation.deliver(job)
    assert length(ServerMock.get_state().cancellations) == 1
  end

  test "authority is checked again in the API mutation owner", ctx do
    ServerMock.put_state(
      Map.put(ServerMock.get_state(), :cancellation_hook, fn ->
        Repo.get!(Batch, ctx.batch.id) |> Batch.changeset(%{state: :running}) |> Repo.update!()
      end)
    )

    assert :ok == CandidateCancellation.deliver(claim(ctx.job))
    refute ServerMock.get_state()[:cancellations]
  end

  test "completed, missing and denied runs are explicit terminal observations", ctx do
    for outcome <- ["success", "cancelled", "failure"] do
      assert {:terminal, ^outcome} =
               CandidateCancellation.validate_run(
                 Map.merge(ctx.run, %{"status" => "completed", "conclusion" => outcome}),
                 ctx.p
               )
    end

    ServerMock.put_state(%{cancellation_lookup_error: {:error, :dispatch_lookup, 403}})
    assert :ok == CandidateCancellation.deliver(claim(ctx.job))
    refute ServerMock.get_state()[:cancellations]
  end
end
