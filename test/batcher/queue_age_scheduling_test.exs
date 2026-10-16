defmodule BorsNG.Worker.QueueAgeSchedulingTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Batch, Installation, LinkPatchBatch, Patch, Project, Repo}
  alias BorsNG.GitHub
  alias BorsNG.Worker.{Batcher, CandidateJournal}

  for guard <- [:none, :poll, :recovery, :promotion] do
    @guard guard
    test "natural poll selects oldest eligible admission with #{@guard} guard" do
      inst = Repo.insert!(%Installation{installation_xref: 91})

      project =
        Repo.insert!(%Project{
          name: "example/project",
          installation_id: inst.id,
          repo_xref: 14,
          staging_branch: "staging"
        })

      conn = {{:installation, 91}, 14}

      GitHub.ServerMock.put_state(%{
        conn => %{
          branches: %{"master" => "ini", "staging" => "", "staging.tmp" => ""},
          commits: %{},
          comments: Map.new(1..3, &{&1, []}),
          statuses: %{},
          files: %{"staging.tmp" => %{"bors.toml" => ~s/status = [ "ci" ]/}},
          pulls:
            Map.new(1..3, fn pr ->
              {pr,
               %GitHub.Pr{
                 number: pr,
                 title: "Test",
                 body: "",
                 state: :open,
                 base_ref: "master",
                 head_sha: "H#{pr}",
                 head_ref: "update",
                 base_repo_id: 14,
                 head_repo_id: 14,
                 merged: false
               }}
            end),
          pr_commits:
            Map.new(
              1..3,
              &{&1, [%GitHub.Commit{sha: "sha#{&1}", author_name: "a", author_email: "e"}]}
            )
        }
      })

      batches =
        for pr <- 1..3 do
          b =
            Repo.insert!(%Batch{
              Batch.new(project.id, "master")
              | sealed: true,
                last_polled: 4 - pr
            })

          patch =
            Repo.insert!(%Patch{
              project_id: project.id,
              pr_xref: pr,
              commit: "H#{pr}",
              into_branch: "master"
            })

          %LinkPatchBatch{}
          |> LinkPatchBatch.changeset(%{
            patch_id: patch.id,
            batch_id: b.id,
            head_sha: patch.commit,
            reviewer: "reviewer"
          })
          |> CandidateJournal.insert_member()

          b
        end

      [oldest, middle, newest] = batches
      now = DateTime.to_unix(DateTime.utc_now())

      changes =
        case @guard do
          :none -> %{}
          :poll -> %{last_polled: now + 3_600}
          :recovery -> %{recovery_after: now + 3_600}
          :promotion -> %{promotion_retry_at: now + 3_600}
        end

      oldest = oldest |> Batch.changeset(changes) |> Repo.update!()
      links_before = Repo.all(LinkPatchBatch)
      assert {:noreply, _} = Batcher.handle_info({:poll, :once}, project.id)
      expected = if @guard == :none, do: oldest, else: middle
      assert Repo.get!(Batch, expected.id).state == :running
      assert Repo.get!(Batch, newest.id).state == :waiting

      if @guard != :none do
        after_poll = Repo.get!(Batch, oldest.id)
        assert after_poll.state == :waiting
        assert after_poll.last_polled == oldest.last_polled
        assert after_poll.recovery_after == oldest.recovery_after
        assert after_poll.promotion_retry_at == oldest.promotion_retry_at
      end

      assert Repo.all(LinkPatchBatch) == links_before

      assert GitHub.ServerMock.get_state()[conn].branches["staging"] ==
               "iniH#{if @guard == :none, do: 1, else: 2}"
    end
  end
end
