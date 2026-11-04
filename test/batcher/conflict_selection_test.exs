defmodule BorsNG.Worker.Batcher.ConflictSelectionTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.Batcher.ConflictSelection

  defp link(pr),
    do: %{head_sha: String.pad_leading(to_string(pr), 40, "0"), patch: %{pr_xref: pr}}

  defp member(l), do: %{"pr" => l.patch.pr_xref, "head_sha" => l.head_sha}

  test "selection must preserve the exact order, heads, and complete atomic units" do
    [a, b, c, d] = Enum.map(1..4, &link/1)
    units = [[a], [b, c], [d]]

    skipped = %{
      "members" => Enum.map([b, c], &member/1),
      "conflict" => member(c) |> Map.put("kind", "merge_conflict")
    }

    valid = %{"accepted" => Enum.map([a, d], &member/1), "skipped" => [skipped]}
    assert ConflictSelection.validate(units, valid) == :ok

    collision = %{
      "members" => [member(d)],
      "conflict" =>
        member(d)
        |> Map.merge(%{
          "kind" => "declaration_collision",
          "name" => "TauCeti.x",
          "module" => "TauCeti.N",
          "other_pr" => 1,
          "other_module" => "TauCeti.M"
        })
    }

    assert ConflictSelection.validate(units, %{
             "accepted" => [member(a)],
             "skipped" => [collision, skipped]
           }) == :ok

    assert ConflictSelection.merge_skips(valid, [collision]) == %{
             "accepted" => Enum.map([a, d], &member/1),
             "skipped" => [collision, skipped],
             "declaration_collisions" => [collision]
           }

    assert ConflictSelection.merge_skips(valid, []) == valid

    for invalid <- [
          %{valid | "accepted" => Enum.map([d, a], &member/1)},
          %{valid | "accepted" => Enum.map([a, b, d], &member/1)},
          %{valid | "accepted" => Enum.map([a, d, d], &member/1)},
          %{valid | "accepted" => [member(a), Map.put(member(d), "head_sha", "changed")]},
          %{valid | "skipped" => [skipped, skipped]},
          %{valid | "skipped" => [%{skipped | "members" => [member(c)]}]},
          %{
            valid
            | "skipped" => [%{skipped | "conflict" => Map.put(skipped["conflict"], "pr", 999)}]
          },
          %{
            valid
            | "skipped" => [
                %{skipped | "conflict" => Map.put(skipped["conflict"], "kind", "other")}
              ]
          },
          %{valid | "skipped" => [nil]},
          %{valid | "skipped" => [%{}]},
          %{}
        ] do
      assert ConflictSelection.validate(units, invalid) == {:error, :invalid_selection}
    end
  end
end

defmodule BorsNG.Worker.Batcher.ConflictSelectionRaceTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Batch, LinkPatchBatch, Patch, PatchBundle, Project, Repo}
  alias BorsNG.Worker.CandidateJournal
  alias BorsNG.Worker.Batcher.ConflictSelection

  setup do
    project = Repo.insert!(%Project{name: "TauCetiProject/TauCeti"})
    batch = Repo.insert!(Batch.new(project.id, "main")) |> Repo.preload(:project)

    for n <- 1..3 do
      head = String.pad_leading(to_string(n), 40, "0")

      patch =
        Repo.insert!(%Patch{
          project_id: project.id,
          pr_xref: n,
          commit: head,
          into_branch: "main",
          manual_approval: %{"head_sha" => head}
        })

      CandidateJournal.insert_member(
        LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
          batch_id: batch.id,
          patch_id: patch.id,
          head_sha: head,
          reviewer: "human",
          manual_approval: patch.manual_approval
        })
      )
    end

    links = Repo.all(LinkPatchBatch.from_batch(batch.id)) |> Enum.sort_by(& &1.patch.pr_xref)
    base = String.duplicate("b", 40)
    identity = ConflictSelection.identity(batch, links, base)
    members = Enum.map(links, &%{"pr" => &1.patch.pr_xref, "head_sha" => &1.head_sha})

    result = %{
      "status" => "ok",
      "complete" => true,
      "identity" => identity,
      "accepted" => [Enum.at(members, 0), Enum.at(members, 2)],
      "skipped" => [
        %{
          "members" => [Enum.at(members, 1)],
          "conflict" => Map.put(Enum.at(members, 1), "kind", "merge_conflict")
        }
      ]
    }

    %{batch: batch, links: links, base: base, identity: identity, result: result}
  end

  for field <- [:commit, :manual_approval, :bundle_id] do
    test "a changed #{field} rejects stale selection without moving any links", ctx do
      [link | _] = ctx.links

      replacement =
        case unquote(field) do
          :commit -> String.duplicate("e", 40)
          :manual_approval -> nil
          :bundle_id -> Repo.insert!(%PatchBundle{project_id: ctx.batch.project_id}).id
        end

      Repo.get!(Patch, link.patch_id)
      |> Ecto.Changeset.change(%{unquote(field) => replacement})
      |> Repo.update!()

      assert {:error, :selection_membership_changed} =
               ConflictSelection.reconcile(
                 ctx.batch,
                 ctx.links,
                 ctx.base,
                 ctx.identity,
                 ctx.result
               )

      assert Repo.aggregate(Batch, :count) == 1
      assert length(Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))) == 3
    end
  end

  test "incomplete or wrong-base observations cannot move queue membership", ctx do
    for result <- [
          %{ctx.result | "complete" => false},
          put_in(ctx.result, ["identity", "base_sha"], String.duplicate("e", 40))
        ] do
      assert {:error, :invalid_selection} =
               ConflictSelection.reconcile(ctx.batch, ctx.links, ctx.base, ctx.identity, result)
    end

    assert Repo.aggregate(Batch, :count) == 1
    assert length(Repo.all(LinkPatchBatch.from_batch(ctx.batch.id))) == 3
  end
end

defmodule BorsNG.Worker.Batcher.ConflictSelectionDeclarationTest do
  use BorsNG.Worker.TestCase
  alias BorsNG.Database.{Batch, CandidateAdmission, LinkPatchBatch, Patch, Project, Repo}
  alias BorsNG.Worker.{CandidateJournal, DeclarationNames}
  alias BorsNG.Worker.Batcher.{ConflictSelection, Divider}

  setup do
    project = Repo.insert!(%Project{name: "TauCetiProject/TauCeti"})
    batch = Repo.insert!(Batch.new(project.id, "main")) |> Repo.preload(:project)

    records = %{
      1 => %{"state" => "known", "added" => %{"TauCeti.M" => ["TauCeti.x"]}},
      2 => %{"state" => "known", "added" => %{"TauCeti.N" => ["TauCeti.x"]}},
      3 => %{"state" => "unknown", "reason" => "records=0"}
    }

    for n <- 1..3 do
      head = String.pad_leading(to_string(n), 40, "0")

      patch =
        Repo.insert!(%Patch{
          project_id: project.id,
          pr_xref: n,
          commit: head,
          into_branch: "main",
          manual_approval: %{"head_sha" => head}
        })

      link =
        CandidateJournal.insert_member(
          LinkPatchBatch.changeset(%LinkPatchBatch{}, %{
            batch_id: batch.id,
            patch_id: patch.id,
            head_sha: head,
            reviewer: "human",
            manual_approval: patch.manual_approval
          })
        )

      admission = CandidateJournal.admission(link.id)

      admission
      |> Ecto.Changeset.change(
        snapshot: Map.put(admission.snapshot, "declaration_names", records[n])
      )
      |> Repo.update!()
    end

    links = Repo.all(LinkPatchBatch.from_batch(batch.id)) |> Enum.sort_by(& &1.patch.pr_xref)
    {:ok, project: project, batch: batch, links: links}
  end

  test "records come from the admissions and the collision skip joins the Git skips in one deferred group",
       c do
    units = Divider.group_units(c.links)
    [a, b, d] = c.links
    assert map_size(DeclarationNames.records(c.links)) == 3

    assert [skip] = ConflictSelection.declaration_skips(units, c.links)
    assert skip["conflict"]["pr"] == 2 and skip["conflict"]["other_pr"] == 1
    assert skip["conflict"]["name"] == "TauCeti.x"

    base = String.duplicate("b", 40)
    identity = ConflictSelection.identity(c.batch, c.links, base)
    member = &%{"pr" => &1.patch.pr_xref, "head_sha" => &1.head_sha}

    git_result = %{
      "status" => "ok",
      "complete" => true,
      "identity" => identity,
      "accepted" => [member.(a)],
      "skipped" => [
        %{"members" => [member.(d)], "conflict" => Map.put(member.(d), "kind", "merge_conflict")}
      ]
    }

    result = ConflictSelection.merge_skips(git_result, [skip])

    assert {:ok, selected, evidence} =
             ConflictSelection.reconcile(c.batch, c.links, base, identity, result)

    assert Enum.map(selected, & &1.patch.pr_xref) == [1]
    assert evidence["declaration_collisions"] == [skip]
    assert is_integer(evidence["deferred_batch_id"])

    deferred = Repo.all(LinkPatchBatch.from_batch(evidence["deferred_batch_id"]))
    assert Enum.sort(Enum.map(deferred, & &1.patch.pr_xref)) == [2, 3]

    # the deferred retry keeps the record of its exact head
    retried = Enum.find(deferred, &(&1.patch.pr_xref == 2))

    assert CandidateJournal.admission(retried.id).snapshot["declaration_names"]["state"] ==
             "known"

    assert Repo.aggregate(CandidateAdmission, :count, :id) == 5
    assert Enum.map(Repo.all(LinkPatchBatch.from_batch(c.batch.id)), & &1.patch.pr_xref) == [1]
    _ = b
  end

  test "the switch is off by default and the pre-check adds nothing without it", c do
    assert DeclarationNames.enabled?() == false
    units = Divider.group_units(c.links)
    assert [_] = ConflictSelection.declaration_skips(units, c.links)
  end
end
