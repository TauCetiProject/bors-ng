defmodule BorsNG.ApiControllerTest do
  use BorsNG.ConnCase

  alias BorsNG.Database.Repo
  alias BorsNG.Database.Installation
  alias BorsNG.Database.Project
  alias BorsNG.Database.Batch

  setup do
    installation = Repo.insert!(%Installation{installation_xref: 101})

    project1 =
      Repo.insert!(%Project{
        installation_id: installation.id,
        repo_xref: 201,
        name: "example/project1"
      })

    project2 =
      Repo.insert!(%Project{
        installation_id: installation.id,
        repo_xref: 202,
        name: "example/project2"
      })

    {:ok, project1: project1, project2: project2}
  end

  test "global active batches endpoint returns only waiting/running across projects", %{
    conn: conn,
    project1: project1,
    project2: project2
  } do
    b1 = Repo.insert!(%Batch{project_id: project1.id, state: :waiting})
    b2 = Repo.insert!(%Batch{project_id: project1.id, state: :running})
    _b3 = Repo.insert!(%Batch{project_id: project1.id, state: :ok})
    b4 = Repo.insert!(%Batch{project_id: project2.id, state: :waiting})

    conn =
      conn
      |> put_req_header("accept", "application/json")
      |> get("/api/active-batches")

    resp = json_response(conn, 200)
    assert Map.has_key?(resp, "batch_ids")
    assert Enum.sort(resp["batch_ids"]) == Enum.sort([b1.id, b2.id, b4.id])
  end

  test "project-scoped active batches endpoint returns only that project's active batches", %{
    conn: conn,
    project1: project1,
    project2: project2
  } do
    b1 = Repo.insert!(%Batch{project_id: project1.id, state: :waiting})
    b2 = Repo.insert!(%Batch{project_id: project1.id, state: :running})
    _b3 = Repo.insert!(%Batch{project_id: project1.id, state: :ok})
    _b4 = Repo.insert!(%Batch{project_id: project2.id, state: :waiting})

    conn =
      conn
      |> put_req_header("accept", "application/json")
      |> get("/repositories/#{project1.id}/active-batches")

    resp = json_response(conn, 200)
    assert Map.has_key?(resp, "batch_ids")
    assert Enum.sort(resp["batch_ids"]) == Enum.sort([b1.id, b2.id])
  end

  test "project-scoped active batches returns 404 for missing project", %{conn: conn} do
    conn =
      conn
      |> put_req_header("accept", "application/json")
      |> get("/repositories/999999/active-batches")

    assert conn.status == 404
  end

  test "queue stops expose only current open heads on the selected project and base", %{
    conn: conn,
    project1: project,
    project2: other
  } do
    alias BorsNG.Database.Patch

    for {number, project_id, base, open, head, state, reason} <- [
          {1, project.id, "main", true, "current", "withdrawn", "requested"},
          {2, project.id, "main", true, "current", "failed", "build"},
          {3, project.id, "main", true, "changed", "withdrawn", "cancel_all"},
          {4, project.id, "pilot", true, "current", "withdrawn", "requested"},
          {5, other.id, "main", true, "current", "withdrawn", "requested"},
          {6, project.id, "main", false, "current", "withdrawn", "requested"},
          {7, project.id, "main", true, "current", "approved", nil}
        ] do
      Repo.insert!(%Patch{
        project_id: project_id,
        pr_xref: number,
        into_branch: base,
        open: open,
        commit: head,
        queue_approval: %{
          "head_sha" => "current",
          "state" => state,
          "reason" => reason,
          "reviewer" => "private"
        }
      })
    end

    response =
      conn |> get("/repositories/#{project.id}/active-batches?base=main") |> json_response(200)

    assert Enum.sort_by(response["queue_stops"], & &1["pr"]) == [
             %{
               "pr" => 1,
               "head_sha" => "current",
               "state" => "withdrawn",
               "reason" => "requested"
             },
             %{"pr" => 2, "head_sha" => "current", "state" => "failed", "reason" => "build"}
           ]
  end

  test "main observation excludes pilots and preserves an approved head after a push", %{
    conn: conn,
    project1: project
  } do
    main = Repo.insert!(%Batch{project_id: project.id, into_branch: "main", state: :waiting})
    pilot = Repo.insert!(%Batch{project_id: project.id, into_branch: "pilot", state: :running})

    patch =
      Repo.insert!(%BorsNG.Database.Patch{
        project_id: project.id,
        pr_xref: 42,
        into_branch: "main",
        commit: "new"
      })

    Repo.insert!(%BorsNG.Database.LinkPatchBatch{
      batch_id: main.id,
      patch_id: patch.id,
      head_sha: "approved",
      reviewer: "r"
    })

    Repo.insert!(%BorsNG.Database.LinkPatchBatch{
      batch_id: pilot.id,
      patch_id: patch.id,
      reviewer: "r"
    })

    conn =
      conn
      |> put_req_header("accept", "application/json")
      |> get("/repositories/#{project.id}/active-batches?base=main&batch_id=#{main.id}")

    response = json_response(conn, 200)
    assert response["batch_ids"] == [main.id]
    assert response["requested_batch"]["id"] == main.id
    assert response["requested_batch"]["members"] == [%{"pr" => 42, "head_sha" => "approved"}]
    assert response["base"] == "main"
    assert response["repo"] == project.name
    assert hd(response["batches"])["members"] == [%{"pr" => 42, "head_sha" => "approved"}]
    assert hd(response["outcomes"])["head_sha"] == "approved"
    assert get_resp_header(conn, "cache-control") == ["no-store"]
    [link] = Repo.all(BorsNG.Database.LinkPatchBatch.from_batch(main.id))
    cloned = BorsNG.Worker.Batcher.Divider.clone_batch([link], project.id, "main")
    assert Repo.one!(BorsNG.Database.LinkPatchBatch.from_batch(cloned.id)).head_sha == "approved"
  end

  test "observation exposes exact human approvals and staging bases, excluding stale heads", %{
    conn: conn,
    project1: p
  } do
    head = String.duplicate("a", 40)
    base = String.duplicate("b", 40)
    approval = %{"actor_id" => 477_956, "head_sha" => head, "merge_base_sha" => base}

    patch =
      Repo.insert!(%BorsNG.Database.Patch{
        project_id: p.id,
        pr_xref: 42,
        into_branch: "main",
        commit: head,
        manual_approval: approval
      })

    batch =
      Repo.insert!(%Batch{project_id: p.id, state: :running, into_branch: "main", base_sha: base})

    path = "/repositories/#{p.id}/active-batches?base=main&batch_id=#{batch.id}"
    response = conn |> get(path) |> json_response(200)
    assert response["scope_approvals"] == [Map.put(approval, "pr", 42)]
    assert response["requested_batch"]["base_sha"] == base
    patch |> BorsNG.Database.Patch.changeset(%{commit: base}) |> Repo.update!()
    response = conn |> recycle() |> get(path) |> json_response(200)
    assert response["scope_approvals"] == []
  end

  test "PR lookup selects its newest current-head batch on the requested base", %{
    conn: conn,
    project1: project
  } do
    alias BorsNG.Database.{Patch, LinkPatchBatch}

    patch =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: 42,
        into_branch: "main",
        commit: "current"
      })

    for {branch, head, state} <- [
          {"main", "old", :error},
          {"main", "current", :error},
          {"pilot", "current", :running}
        ] do
      batch = Repo.insert!(%Batch{project_id: project.id, into_branch: branch, state: state})

      Repo.insert!(%LinkPatchBatch{
        patch_id: patch.id,
        batch_id: batch.id,
        head_sha: head,
        reviewer: "r"
      })
    end

    response =
      conn
      |> get("/repositories/#{project.id}/active-batches?base=main&pr=42")
      |> json_response(200)

    assert response["requested_batch"]["state"] == "error"
    assert response["requested_batch"]["base"] == "main"
    assert response["requested_batch"]["members"] == [%{"pr" => 42, "head_sha" => "current"}]
    patch |> Patch.changeset(%{commit: "new"}) |> Repo.update!()

    response =
      conn
      |> recycle()
      |> get("/repositories/#{project.id}/active-batches?base=main&pr=42")
      |> json_response(200)

    assert response["requested_batch"] == nil
  end
end
