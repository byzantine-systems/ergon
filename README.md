# Ergon

[![Built with Nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)
[![[Nix] Build & Test](https://github.com/byzantine-systems/ergon/actions/workflows/build.yml/badge.svg)](https://github.com/byzantine-systems/ergon/actions/workflows/build.yml)
![License](https://img.shields.io/github/license/byzantine-systems/ergon)

> [!WARNING]
> This project is under active development. Avoid using it for production apps.

A library for PostgreSQL-native background job and workflow processing in Elixir with the simple premise that a recent PostgreSQL is enough on its own, so:

- No Redis.
- No separate Graph Database.
- No third-party job DSL.

Everything leans on a couple database capabilities:

- [Temporal Tables](https://www.postgresql.org/docs/19/ddl-temporal-tables.html) + [Temporal Constraints](https://neon.com/postgresql/18/temporal-constraints): Unique jobs enforced by a temporal PK, auditable history via `FOR PORTION OF` updates rather than in-place overwrites.
- **`UPDATE ... FOR PORTION OF`** (see [Updating and Deleting Temporal Data](https://www.postgresql.org/docs/19/dml-application-time-update-delete.html)), closes a job's validity window and writes a fresh row for the new state.
- SQL/PGG [Property Graphs](https://www.postgresql.org/docs/19/ddl-property-graphs.html): DAG dependencies resolved with a single `GRAPH_TABLE`/`MATCH` query instead of recursive CTEs.

Ergon also ships generic infrastructure for the common pg-product stack: **pgmq** (durable queues backing Broadway producers), **pg_cron** (guarded schedule helpers), and monthly **partition lifecycle** management.

> [!NOTE]
> Ergon targets **PostgreSQL 19** (see `flake.nix`). It uses recent features that won't run on older Postgres instances.

## Quick start

### 1. Add the dependency

```elixir
defp deps do
  [{:ergon, "~> 0.1"}]
end
```

### 2. Configure the repo

```elixir
# config/config.exs
config :ergon, ecto_repos: [Ergon.Repo]

config :ergon, Ergon.Repo,
  types: Ergon.PostgresTypes
```

```elixir
# config/runtime.exs
config :ergon, Ergon.Repo,
  hostname: System.get_env("PGHOST", "127.0.0.1"),
  port: 5432,
  username: System.get_env("PGUSER"),
  password: System.get_env("PGPASSWORD"),
  database: System.get_env("PGDATABASE")
```

### 3. Install extensions and run the migrations

Ergon's migrations install the extensions they need (`btree_gist`, `pgmq`, conditionally `pg_cron`) via `Ergon.Migration.extensions/0`, call it from your own init migration, or just run the bundled ones:

```bash
mix ecto.setup
```

## A first taste

Enqueue a job, then start a worker to drain its queue. Ergon handles checkout
(`FOR UPDATE SKIP LOCKED`), execution, retries, and the state transition:

```elixir
{:ok, _job} =
  Ergon.NewJob.new("send_email", %{to: "alice@example.com", body: "..."})
  |> Ergon.NewJob.on_queue("mailers")
  |> Ergon.enqueue()

def handle(%Ergon.Job{payload: json}) do
  %{to: to, body: body} = Jason.decode!(json, keys: :atoms)
  MyApp.Mailer.send(to, body)
  :ok
end

{:ok, _worker} =
  Ergon.Queue.new("mailers")
  |> Ergon.start_worker(&handle/1)
```

The handler returns `:ok` to complete the job, or `{:error, reason}` to record
the reason and retry until `max_attempts` is exhausted. That is the whole loop.

## Examples and guides

The [`examples/`](examples/) directory holds task-oriented guides, each also
published under **Guides** in the [generated docs](https://hexdocs.pm/ergon):

- [Getting started](examples/getting-started.md), the SKIP LOCKED worker path in full: enqueue, run a worker, tune the queue.
- [Unique jobs](examples/unique-jobs.md), database-enforced deduplication with a temporal constraint.
- [Workflows](examples/workflows.md), DAG dependencies resolved by a SQL/PGQ property graph.
- [pgmq + Broadway](examples/pgmq-broadway.md), high-throughput streaming with backpressure and at-least-once delivery.
- [Migration helpers](examples/migrations.md), reuse Ergon's PostgreSQL patterns: extensions, bi-temporal tables, graph tables, queues, partitions.
- [Scheduling](examples/scheduling.md), recurring SQL with pg_cron, and why Ergon prefers cron ticks to row triggers.
- [Operations](examples/operations.md), the job notifier, boot-time partition safety, health checks, and disaster recovery.
