defmodule BorsNG.BundleControllerTest do
  use BorsNG.ConnCase

  alias BorsNG.Database.Batch
  alias BorsNG.Database.Installation
  alias BorsNG.Database.LinkPatchBatch
  alias BorsNG.Database.Patch
  alias BorsNG.Database.PatchBundle
  alias BorsNG.Database.Project
  alias BorsNG.Database.Repo
  alias BorsNG.Database.User

  setup do
    installation =
      Repo.insert!(%Installation{
        installation_xref: 31
      })

    project =
      Repo.insert!(%Project{
        installation_id: installation.id,
        repo_xref: 13,
        name: "example/project"
      })

    user =
      Repo.insert!(%User{
        user_xref: 23,
        login: "ghost"
      })

    bundle = Repo.insert!(%PatchBundle{project_id: project.id})

    base =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: 43,
        title: "base patch",
        bundle_id: bundle.id,
        bundle_reviewer: "reviewer"
      })

    stacked =
      Repo.insert!(%Patch{
        project_id: project.id,
        pr_xref: 44,
        title: "stacked patch",
        bundle_id: bundle.id,
        stacked_on_id: base.id
      })

    {:ok, project: project, user: user, bundle: bundle, base: base, stacked: stacked}
  end

  test "shows the bundle without logging in", %{conn: conn, bundle: bundle} do
    conn = get(conn, "/bundles/#{bundle.id}")
    html = html_response(conn, 200)
    assert html =~ "Bundle Details"
    assert html =~ "#44"
  end

  test "needs a login when private repositories are allowed", %{conn: conn, bundle: bundle} do
    with_private_repos(fn ->
      conn = get(conn, "/bundles/#{bundle.id}")
      assert html_response(conn, 302) =~ "auth"
    end)
  end

  defp with_private_repos(fun) do
    previous = System.get_env("ALLOW_PRIVATE_REPOS")
    System.put_env("ALLOW_PRIVATE_REPOS", "true")

    try do
      fun.()
    after
      if previous,
        do: System.put_env("ALLOW_PRIVATE_REPOS", previous),
        else: System.delete_env("ALLOW_PRIVATE_REPOS")
    end
  end

  def login(conn) do
    conn = get(conn, auth_path(conn, :index, "github"))
    assert html_response(conn, 302) =~ "MOCK_GITHUB_AUTHORIZE_URL"

    conn =
      get(
        conn,
        auth_path(
          conn,
          :callback,
          "github",
          %{"code" => "MOCK_GITHUB_AUTHORIZE_CODE"}
        )
      )

    html_response(conn, 302)
    conn
  end

  test "shows the members in stack order with their approval state", %{
    conn: conn,
    bundle: bundle
  } do
    conn = login(conn)
    conn = get(conn, "/bundles/#{bundle.id}")

    html = html_response(conn, 200)
    assert html =~ "Bundle Details"
    assert html =~ "(stack order)"
    assert html =~ "Waiting for approval of"
    assert html =~ "#43"
    assert html =~ "#44"
    assert html =~ "✓ held (reviewer)"
    assert html =~ "waiting"
    assert html =~ "Stacked on #43"
    assert html =~ "None"
    # base before stacked
    assert html =~ ~r/#43.*#44/s
  end

  test "lists the batches the bundle entered", %{conn: conn, bundle: bundle, project: project} do
    batch =
      Repo.insert!(%Batch{
        project_id: project.id,
        state: :running
      })

    for patch <- Repo.all(Patch.all_for_bundle(bundle.id)) do
      Repo.insert!(%LinkPatchBatch{patch_id: patch.id, batch_id: batch.id})
    end

    conn = login(conn)
    conn = get(conn, "/bundles/#{bundle.id}")

    html = html_response(conn, 200)
    assert html =~ "Batch #{batch.id}"
    assert html =~ "(Running)"
    refute html =~ "None"
  end

  test "returns 404 when there is no such bundle", %{conn: conn} do
    conn = login(conn)

    assert_error_sent(404, fn ->
      get(conn, "/bundles/0")
    end)
  end
end
