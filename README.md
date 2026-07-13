# Ergon

[![Built with Nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)
[![[Nix] Build & Test](https://github.com/byzantine-systems/ergon/actions/workflows/build.yml/badge.svg)](https://github.com/byzantine-systems/ergon/actions/workflows/build.yml)

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

## Ergon in 5 minutes, the SKIP LOCKED worker path

The simplest way to use ergon is the job table + polling worker. You enqueue jobs, start a worker per queue, and ergon handles the rest.

```elixir
defmodule MyApp.Mailers do
  # 1. Enqueue a job. `NewJob` is a pipe-friendly builder.
  {:ok, _job} =
    Ergon.NewJob.new("send_email", %{to: "alice@example.com", body: "..."})
    |> Ergon.NewJob.on_queue("mailers")
    |> Ergon.enqueue()

  # 2. Start a worker for the queue. Each worker polls on an interval,
  #    checks out a batch via FOR UPDATE SKIP LOCKED, runs the handler on
  #    each job, and persists the state transition.
  def handle(%Ergon.Job{payload: json}) do
    %{to: to, body: body} = Jason.decode!(json, keys: :atoms)
    MyApp.Mailer.send(to, body)
    :ok
  end

  {:ok, _worker} =
    Ergon.Queue.new("mailers")
    |> Ergon.start_worker(&handle/1)
end
```

The handler returns `:ok` to complete the job, or `{:error, reason}` to record the reason and retry until `max_attempts` is exhausted.

### Unique jobs

A unique job hashes `(queue, worker, payload)` so duplicates collide on the temporal `UNIQUE (fingerprint, valid_period WITHOUT OVERLAPS)` constraint. Configure the window:

```elixir
Ergon.NewJob.new("nightly_report", %{date: Date.utc_today()})
|> Ergon.NewJob.unique_for(3600)   # one delivery per hour
|> Ergon.enqueue()
```

A non-unique job (the default) salts the hash with random bytes so duplicates never collide.

### Workflow dependencies

Wire up DAG dependencies via the SQL/PGQ property graph:

```elixir
{:ok, build} = Ergon.enqueue(Ergon.NewJob.new("build"))
{:ok, deploy} = Ergon.enqueue(Ergon.NewJob.new("deploy"))

:ok = Ergon.depends_on(build.id, deploy.id)

# deploy shows up in ready_children/0 only after build has completed:
{:ok, []} = Ergon.ready_children()
```

## The pgmq + Broadway path

For higher-throughput streaming, use `Ergon.Pgmq.Producer` as a Broadway producer over a pgmq queue. pgmq is the durable buffer; the producer polls it and wraps each message in a `Broadway.Message`.

### 1. Create the queue

```elixir
# In a migration:
import Ergon.Migration

pgmq_queue(:telemetry_processing)
```

### 2. Install the release-leases helper

Ergon ships a plpgsql function for DR recovery, stranded visibility leases are force-expired so messages become immediately re-readable:

```elixir
# Already installed by Ergon's bundled migration
# 20260721000003_install_pgmq_release_leases.exs
```

### 3. Use the producer in a Broadway pipeline

```elixir
defmodule MyApp.Pipeline do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {Ergon.Pgmq.Producer,
           queue: "telemetry_processing",
           poll_interval: 100,
           visibility_timeout: 30,
           notify_channel: "pgmq_telemetry_processing"},
        concurrency: 1
      ],
      processors: [default: [concurrency: 50]]
    )
  end

  @impl Broadway
  def handle_message(:default, %Broadway.Message{data: payload} = msg, _ctx) do
    MyApp.Telemetry.ingest(payload)
    msg
  end
end
```

The producer is the source of truth, LISTEN is a latency optimisation only. A dropped `NOTIFY` costs latency, never events: pgmq is the durable buffer, and the poll loop keeps delivering regardless.

### Disaster recovery

When a consumer dies mid-message, the visibility timeout eventually redelivers it. To force recovery immediately:

```elixir
Ergon.Reconciler.run(
  queues: ~w(telemetry_processing),
  hydrate: &MyApp.State.stop_all_and_rebuild/0
)
```

## Generic migration helpers

`import Ergon.Migration` in any Ecto migration for the patterns ergon uses itself:

```elixir
defmodule MyApp.Repo.Migrations.Setup do
  use Ecto.Migration
  import Ergon.Migration

  def change do
    extensions()                              # btree_gist + pgmq + conditionally pg_cron
    versioning_trigger()                      # shared temporal_versioning() function

    bitemporal_table(:assets, "name text NOT NULL, state text NOT NULL DEFAULT 'idle'")
    pgmq_queue(:asset_events)

    partitioned_table(:asset_telemetry_pings, :recorded_at)
  end
end
```

For pg_cron scheduling, `import Ergon.Cron`:

```elixir
defmodule MyApp.Repo.Migrations.ScheduleReports do
  use Ecto.Migration
  import Ergon.Cron

  def up do
    schedule("hourly-report", "0 * * * *", "SELECT hourly_report()")
  end

  def down do
    unschedule("hourly-report")
  end
end
```

Both are guarded, they're no-ops where the underlying extension isn't installed, so the same migration runs cleanly in dev (extensions active) and test (pg_cron absent by design).

## Boot-time safety

For partitioned tables, add `Ergon.PartitionBootCheck` to your supervision tree. It runs in `init/1` and blocks the rest of the tree from starting until the next N months of partitions are verified:

```elixir
children = [
  {Ergon.PartitionBootCheck, table: :asset_telemetry_pings, months_ahead: 2}
]
```

## Health

```elixir
config :ergon, queues: ~w(telemetry_processing geofence_alerts)

Ergon.Health.check()
# => %{
#   db: {:ok, _},
#   extensions: %{"pgmq" => "1.5.0", "btree_gist" => "1.5", ...},
#   queues: %{"telemetry_processing" => %{queue_length: 5, ...}}
# }
```
