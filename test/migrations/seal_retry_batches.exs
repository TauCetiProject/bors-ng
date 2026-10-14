# Run against a NEW disposable database (never production):
# MIX_ENV=test DATABASE_URL_TEST=postgresql://.../empty_db mix run --no-start test/migrations/seal_retry_batches.exs
# For MySQL, set BORS_DATABASE=mysql and override RepoMysql's test database first.
import ExUnit.Assertions
alias BorsNG.Database.{Batch, Installation, Patch, Project, Repo}

assert Mix.env() == :test
Application.ensure_all_started(:ecto_sql)
Application.ensure_all_started(:postgrex)
Application.ensure_all_started(:myxql)
BorsNG.Application.set_repo()
repo = BorsNG.Application.fetch_repo()
config = Confex.Resolver.resolve!(repo.config())
repo.__adapter__().storage_up(config)
{:ok, _} = repo.start_link(pool: DBConnection.ConnectionPool)

# Refuse to change a database that already has applied bors migrations.
refute Ecto.Migrator.migrations(repo, "priv/repo/migrations")
       |> Enum.any?(&match?({:up, _, _}, &1))

Ecto.Migrator.run(repo, "priv/repo/migrations", :up, to: 20_261_012_010_000)
inst = Repo.insert!(%Installation{installation_xref: 91})

project =
  Repo.insert!(%Project{name: "TauCetiProject/TauCeti", repo_xref: 14, installation_id: inst.id})

other = Repo.insert!(%Project{name: "other/repo", repo_xref: 15, installation_id: inst.id})
now = ~N[2026-10-13 00:00:00]

for {project_id, branch, state} <- [
      {project.id, "main", 0},
      {project.id, "main", 1},
      {project.id, "pilot", 0},
      {project.id, "main", 3},
      {other.id, "main", 0}
    ] do
  Repo.insert_all("batches", [
    %{
      project_id: project_id,
      into_branch: branch,
      state: state,
      priority: 7,
      last_polled: 123,
      inserted_at: now,
      updated_at: now
    }
  ])
end

p =
  Repo.insert!(%Patch{
    project_id: project.id,
    pr_xref: 1,
    commit: "new-head",
    into_branch: "main",
    manual_approval: %{"head_sha" => "approved-head", "actor_id" => 42},
    queue_approval: %{"head_sha" => "approved-head", "reviewer" => "r"},
    queue_pending: true
  })

Repo.insert!(%BorsNG.Database.LinkPatchBatch{
  batch_id: 1,
  patch_id: p.id,
  reviewer: "r",
  head_sha: "approved-head",
  manual_approval: p.manual_approval
})

snapshot = fn ->
  for sql <- [
        "SELECT id, project_id, into_branch, state, priority, last_polled, inserted_at, updated_at FROM batches ORDER BY id",
        "SELECT * FROM link_patch_batch ORDER BY id",
        "SELECT * FROM patches ORDER BY id"
      ],
      do: Ecto.Adapters.SQL.query!(repo, sql).rows
end

before = snapshot.()
assert Ecto.Migrator.run(repo, "priv/repo/migrations", :up, all: true) == [20_261_014_000_000]
assert snapshot.() == before
assert Enum.map(1..5, &Repo.get!(Batch, &1).sealed) == [true, true, false, false, false]
normal = Repo.insert!(Batch.new(project.id, "main", 7))
refute normal.sealed
assert Ecto.Migrator.run(repo, "priv/repo/migrations", :up, all: true) == []
refute Repo.get!(Batch, normal.id).sealed
IO.puts("PASS: legacy seals, unchanged membership/priority/approvals, and restart-safe migration")
