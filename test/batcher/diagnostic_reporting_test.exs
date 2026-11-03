defmodule BorsNG.Worker.DiagnosticReportingTest do
  use BorsNG.Worker.TestCase

  alias BorsNG.Database.{
    Batch,
    CandidateEvent,
    Installation,
    LinkPatchBatch,
    OrchestrationJob,
    Patch,
    Project,
    Repo
  }

  alias BorsNG.Worker.{CandidateJournal, DiagnosticReporting}
  alias BorsNG.Worker.Orchestration.Outbox

  setup do
    :sys.suspend(BorsNG.Worker.Orchestration.Dispatcher)
    previous = System.get_env("BORS_ASYNC_ORCHESTRATION")
    System.put_env("BORS_ASYNC_ORCHESTRATION", "true")

    on_exit(fn ->
      if previous,
        do: System.put_env("BORS_ASYNC_ORCHESTRATION", previous),
        else: System.delete_env("BORS_ASYNC_ORCHESTRATION")

      :sys.resume(BorsNG.Worker.Orchestration.Dispatcher)
    end)

    installation = Repo.insert!(%Installation{installation_xref: 91})

    project =
      Repo.insert!(%Project{
        name: "TauCetiProject/TauCeti",
        repo_xref: 14,
        installation_id: installation.id
      })

    batch = Repo.insert!(Batch.new(project.id, "main"))

    links =
      for n <- 1..2 do
        patch =
          Repo.insert!(%Patch{
            project_id: project.id,
            pr_xref: n,
            commit: "head#{n}",
            open: true,
            into_branch: "main"
          })

        CandidateJournal.insert_member(
          LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
            patch_id: patch.id,
            batch_id: batch.id,
            head_sha: patch.commit,
            reviewer: "reviewer"
          })
        )
      end

    candidate = CandidateJournal.begin(batch, Repo.preload(links, :patch), "base")
    {:ok, project: project, batch: batch, candidate: candidate}
  end

  test "degradation is durable, coalesced and recovered with a notifying follow-up", c do
    result = %{
      "complete" => false,
      "reason" => "transport_timeout",
      "parser_version" => "streaming-v2"
    }

    DiagnosticReporting.record(c.candidate, result, false)
    DiagnosticReporting.record(c.candidate, result, false)

    assert Repo.aggregate(
             from(j in OrchestrationJob, where: j.kind == "diagnostic_alert"),
             :count
           ) == 1

    incident = "#{c.candidate.id}:transport_timeout"

    assert :ok =
             DiagnosticReporting.deliver(c.project.id, incident, fn message, options ->
               assert message =~ "1 unresolved of 1"
               assert options[:message_id] == nil
               {:ok, 123}
             end)

    assert DiagnosticReporting.summary(c.project.id).unresolved_candidates == 1

    DiagnosticReporting.record(
      c.candidate,
      %{"complete" => true, "parser_version" => "streaming-v2"},
      true
    )

    assert :ok =
             DiagnosticReporting.deliver(c.project.id, incident, fn message, options ->
               assert message =~ "collection recovered"
               assert options[:message_id] == nil
               {:ok, 123}
             end)

    assert DiagnosticReporting.summary(c.project.id).unresolved_candidates == 0
  end

  test "Zulip requires HTTP success, successful JSON and a real message receipt" do
    alias BorsNG.Worker.Zulip

    assert {:ok, 12} =
             Zulip.delivery_result({:ok, %{status: 200, body: ~s({"result":"success","id":12})}})

    for response <- [
          {:ok, %{status: 500, body: ~s({"result":"success","id":12})}},
          {:ok, %{status: 200, body: ~s({"result":"error","id":12})}},
          {:ok, %{status: 200, body: ~s({"result":"success"})}},
          {:ok, %{status: 200, body: "not JSON"}},
          {:error, :timeout}
        ],
        do: assert(match?({:error, _}, Zulip.delivery_result(response)))
  end

  test "complete unlocated collection closes transport incident and distinguishes mapping", c do
    DiagnosticReporting.record(
      c.candidate,
      %{"complete" => false, "reason" => "transport_timeout"},
      false
    )

    DiagnosticReporting.record(c.candidate, %{"complete" => true}, false)

    assert :ok =
             DiagnosticReporting.deliver(
               c.project.id,
               "#{c.candidate.id}:transport_timeout",
               fn message, _ ->
                 assert message =~ "collection recovered"
                 {:ok, 13}
               end
             )

    assert DiagnosticReporting.batch(c.batch.id).degraded_reason == "unlocated_failure"
    assert DiagnosticReporting.summary(c.project.id).affected_batches == 1
  end

  test "collection has an independent lane from notices", c do
    retry = Outbox.enqueue(c.project.id, "diagnostics", {}, "retry-lane")
    notice = Outbox.enqueue(c.project.id, "notice", {}, "notice-lane")
    assert {:ok, %{id: id}} = Outbox.claim(retry.id)
    assert id == retry.id
    assert {:ok, %{id: notice_id}} = Outbox.claim(notice.id)
    assert notice_id == notice.id
  end

  test "recovery arriving during delivery survives the running job receipt", c do
    DiagnosticReporting.record(
      c.candidate,
      %{"complete" => false, "reason" => "transport_timeout"},
      false
    )

    [job] = Repo.all(from(j in OrchestrationJob, where: j.kind == "diagnostic_alert"))
    assert {:ok, claimed} = Outbox.claim(job.id)
    DiagnosticReporting.record(c.candidate, %{"complete" => true}, true)
    Outbox.finish(claimed)
    assert [%{kind: "diagnostic_alert"}] = Outbox.due(32, :notification)
  end

  test "missing Zulip config cannot count as delivery or block a normal notice", c do
    DiagnosticReporting.record(
      c.candidate,
      %{"complete" => false, "reason" => "permission_denied"},
      false
    )

    assert catch_throw(
             DiagnosticReporting.deliver(
               c.project.id,
               "#{c.candidate.id}:permission_denied",
               fn _, _ -> {:error, :not_configured} end
             )
           ) == {:diagnostic_retry, :not_configured}

    refute Repo.exists?(from(e in CandidateEvent, where: e.kind == "diagnostic_alert_delivered"))

    Repo.update_all(from(j in OrchestrationJob, where: j.kind == "diagnostic_alert"),
      set: [available_at: Outbox.now() + 3600]
    )

    notice = Outbox.enqueue(c.project.id, "notice", {:test_notice}, "test-notice")
    assert Enum.any?(Outbox.due(32, :notification), &(&1.id == notice.id))
    assert {:ok, claimed} = Outbox.claim(notice.id)
    assert claimed
  end

  test "diagnostic retry backoff cannot block other notifications or exhaust permanently", c do
    retry =
      Outbox.enqueue(c.project.id, "diagnostics", {c.batch.id, %{}}, "retry", Outbox.now() + 3600)

    notice = Outbox.enqueue(c.project.id, "notice", {:test_notice}, "notice")
    assert Enum.any?(Outbox.due(32, :notification), &(&1.id == notice.id))

    Repo.update_all(from(j in OrchestrationJob, where: j.id == ^retry.id),
      set: [available_at: 0, attempts: 6]
    )

    assert {:ok, claimed} = Outbox.claim(retry.id)
    assert claimed.attempts == 7
    Outbox.fail(claimed, :transport_failure)
    assert Repo.get!(OrchestrationJob, retry.id).state == "pending"
  end
end
