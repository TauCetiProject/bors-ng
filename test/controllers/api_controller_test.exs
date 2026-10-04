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
      |> get("/repositories/#{project.id}/active-batches?base=main")

    response = json_response(conn, 200)
    assert response["batch_ids"] == [main.id]
    assert response["base"] == "main"
    assert response["repo"] == project.name
    assert hd(response["batches"])["members"] == [%{"pr" => 42, "head_sha" => "approved"}]
    assert hd(response["outcomes"])["head_sha"] == "approved"
    assert get_resp_header(conn, "cache-control") == ["no-store"]
    [link] = Repo.all(BorsNG.Database.LinkPatchBatch.from_batch(main.id))
    cloned = BorsNG.Worker.Batcher.Divider.clone_batch([link], project.id, "main")
    assert Repo.one!(BorsNG.Database.LinkPatchBatch.from_batch(cloned.id)).head_sha == "approved"
  end
end
