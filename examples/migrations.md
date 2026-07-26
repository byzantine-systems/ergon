# Migration helpers

`import Ergon.Migration` in any Ecto migration to reuse the exact PostgreSQL
patterns Ergon is built on: extensions, the shared versioning trigger, bi-temporal
tables, property-graph vertex/edge tables, pgmq queues, and partition lifecycle
functions. Each helper calls `Ecto.Migration.execute/{1,2}` directly, so it
behaves like native Ecto DSL, reversible in `change/0` where it can be.

## A representative setup migration

```elixir
defmodule MyApp.Repo.Migrations.Setup do
  use Ecto.Migration
  import Ergon.Migration

  def change do
    extensions()                              # btree_gist + pgcrypto + pgmq + conditionally pg_cron
    versioning_trigger()                      # shared temporal_versioning() function

    bitemporal_table(:assets, "name text NOT NULL, state text NOT NULL DEFAULT 'idle'")
    pgmq_queue(:asset_events)

    partitioned_table(:asset_telemetry_pings, :recorded_at)
  end
end
```

## `extensions/0`

Installs, idempotently, everything Ergon depends on:

- **`btree_gist`**, required for temporal `WITHOUT OVERLAPS` keys.
- **`pgcrypto`**, the IMMUTABLE `digest(…, 'sha256')` behind the generated
  `fingerprint` column.
- **`pgmq`**, durable queue transport for `Ergon.Pgmq.*`.
- **`pg_cron`**, installed **only** when the current database matches
  `cron.database_name`. pg_cron can be created in exactly one database per
  cluster, so it's skipped elsewhere. This is what lets the same migration run
  cleanly against dev (pg_cron present) and test (absent).

## `versioning_trigger/0` and `bitemporal_table/2`

`versioning_trigger/0` installs the shared, column-agnostic
`temporal_versioning()` function once per database. It inspects the firing table
at run time and archives the `OLD` row into `<table>_history` by naming
convention.

`bitemporal_table/2` then creates a table with application-time (`valid_time`)
and system-time versioning, its `_history` twin, a GiST index, and the trigger:

```elixir
versioning_trigger()   # once, in an early migration
bitemporal_table(:assets, "name text NOT NULL, state text NOT NULL DEFAULT 'idle'")
```

The `id` comes from a bare sequence, not `GENERATED ALWAYS AS IDENTITY`: the
temporal PK `(id, valid_time WITHOUT OVERLAPS)` means one entity legitimately
spans several validity rows sharing one id.

> `bitemporal_table/2` does **not** call `versioning_trigger/0` for you, the
> function must exist before the trigger is attached. Call it once in an early
> migration, then `bitemporal_table/2` as many times as you like.

## Property-graph tables

Build DAGs over your own domain (mirroring what Ergon does for `ergon.jobs`):

```elixir
# Vertex tables, identity registries, one row per logical entity:
vertex_table(:hub_vertices, references: {:id, :hubs})     # FK to a domain table
vertex_table(:asset_vertices)                              # owns its identity
vertex_table(:route_vertices, extra_columns: "code text NOT NULL UNIQUE")

# Edge tables, bi-temporal, with the _history twin and trigger:
edge_table(:routes, {:from_id, :hub_vertices}, {:to_id, :hub_vertices},
  check: "from_id <> to_id")   # ban self-loops
```

## pgmq queues

```elixir
pgmq_queue(:telemetry_processing)        # reversible: down drops the queue
pgmq_notify_cron(:telemetry_processing)  # the LISTEN fast-path tick
```

See [pgmq + Broadway](pgmq-broadway.md) for the consumer side, and
[Scheduling](scheduling.md) for why the notify path is a cron tick.

## Partition lifecycle

`partitioned_table/2` installs an `auto_manage_partitions_<table>()` function
that creates missing monthly partitions, and schedules it weekly via pg_cron:

```elixir
partitioned_table(:asset_telemetry_pings, :recorded_at)
```

The **parent** table (`CREATE TABLE … PARTITION BY RANGE (…)`) is yours to
create, the host owns the schema. Pair this with `Ergon.PartitionBootCheck` in
your supervision tree so partitions are verified at boot (see
[Operations](operations.md#boot-time-partition-safety)).

## Everything is guarded

The pg_cron-dependent helpers are no-ops where pg_cron isn't installed, so the
same migration runs cleanly in dev (extensions active) and test (pg_cron absent
by design). See [Scheduling](scheduling.md) for the guard mechanism.
