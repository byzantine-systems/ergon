# Migration Helpers

Two separate things: running your migrations alongside Ergon's, and generating DDL that reuses Ergon's PostgreSQL patterns for your own tables.

## Ergon's own schema

Ergon installs its schema itself, from `.sql` files under `priv/migrations/`, applied by `migraterl`:

```erlang
{ok, Summary} = ergon_migrate:migrate().
%% dry run
{ok, Plan} = ergon_migrate:plan().
{ok, State} = ergon_migrate:status().
```

The sources apply in a fixed order:

| Source | Class | Contents |
|---|---|---|
| `bootstrap/` | `once` | extensions, `CREATE SCHEMA ergon` |
| `functions/` | `on_change` | routines the schema attaches as triggers |
| `schema/` | `once` | tables, indexes, constraints, RLS, property graph |
| `routines/` | `on_change` | routines that depend on the schema |
| `cron/` | `always` | the notifier ticks |

`functions/` must precede `schema/` because the triggers there reference those functions. `routines/` must follow it, because `ergon.enqueue` names `ergon.jobs` as a return type and `ergon.notify_pending_jobs` has a `LANGUAGE sql` body that PostgreSQL validates at `CREATE` time.

`on_change` is why every routine is `CREATE OR REPLACE`: editing a function body is an edit to its own file rather than a new migration.

## Running your migrations too

Register your own directories, each under **its own schema/namespace**:

```erlang
%% sys.config
{ergon, [
    {ergon_migrate, [
        {extra_sources, [
            #{namespace => ~"my_app",
              sources => [{once, {priv, my_app, "migrations"}}]}
        ]}
    ]}
]}
```

`ergon_migrate:migrate/0` then applies Ergon's namespace first and yours after, so a table of yours that references `ergon.jobs`, or attaches `ergon.temporal_versioning()` as a trigger, always finds them.

The separate namespace is what makes this safe, `migraterl` journals every script under its **basename** and checks ordering against that journal, so sharing Ergon's namespace would put your filenames in competition with `000001..000015` for sort position, and a basename that happened to collide would mark your script as already applied without ever running it. Namespaces are independent journals with their own advisory lock, so number your scripts however you like.

Source directories accept a bare path, `{priv, App, Sub}`, or `{app, App, Sub}`, the same forms `ergon_sql`'s `extra_roots` takes.

> [!WARNING]
> `ergon_migrate:teardown/0` drops **only Ergon's** schema, unlike `migrate/0` which applies everything. Your schema is not Ergon's to drop, and guessing at how to unwind it would be worse than not trying. Tear yours down first if you want a full reset.

### Qualify your DDL

Ergon connects as a role named `ergon`, and PostgreSQL's default `search_path` of `"$user", public` resolves `$user` to the **`ergon` schema**. So this:

```sql
-- Lands in the ergon schema
CREATE TABLE assets (...);
```

creates your table inside Ergon's schema, where `teardown/0` will drop it along with everything else. Write `CREATE TABLE public.assets` (or your own schema), or set a `search_path` at the top of your migration.

The same cascade catches a host table that *depends* on an Ergon type, such as a column typed `ergon.job_state` or a foreign key into an Ergon table. That is ordinary `DROP SCHEMA ... CASCADE` behaviour rather than something Ergon chooses, but it surprises people, so it is worth saying twice.

## Generating DDL

`ergon_migration` returns SQL as `iodata` and executes nothing. Write the output to a `.sql` file in a directory you registered above, and it is applied and journalled like any other migration.

```erlang
SQL = ergon_migration:script(
        ergon_migration:bitemporal_table_sql(~"assets", ~"name text NOT NULL, tag text")),
ok = file:write_file("priv/migrations/000001_assets.sql", SQL).
```

### Bi-Temporal Tables

The pattern `ergon.jobs` itself uses: two periods, a history twin, and a versioning trigger.

```erlang
ergon_migration:bitemporal_table_sql(~"assets", ~"name text NOT NULL")
```

`valid_time` is application time, when the row is true in the world, split by `UPDATE ... FOR PORTION OF`. `system_time` is belief time, maintained by the trigger, with superseded rows archived into `assets_history`. The primary key is temporal: an id is unique at any instant, but one id may own many non-overlapping historical versions.

The history twin is created with `LIKE ... INCLUDING DEFAULTS INCLUDING CONSTRAINTS`, deliberately **not** including indexes or generated columns. **History is an append-only log** the trigger writes verbatim, so a generated column there would refuse the write and a copied temporal primary key would reject the very overlaps history exists to record.

You can attach versioning to an existing table on its own:

```erlang
ergon_migration:history_twin_sql(~"assets"),
ergon_migration:versioning_trigger_sql(~"assets")
```

The function it attaches, `ergon.temporal_versioning()`, is installed by Ergon and is column-agnostic: it inspects the firing table at run time and finds the history twin by naming convention. You define nothing.

### Property Graph element tables

```erlang
ergon_migration:vertex_table_sql(
    ~"hub_vertices", 
    #{references => {~"id", ~"hubs"}}
),
ergon_migration:vertex_table_sql(
    ~"route_vertices", 
    #{extra_columns => ~"code text NOT NULL UNIQUE"}
),
ergon_migration:edge_table_sql(
    ~"routes", 
    {~"from_id", ~"hub_vertices"}, {~"to_id", ~"hub_vertices"},
    #{check => ~"from_id <> to_id"}
)
```

One lesson from Ergon's own graph is worth repeating: whatever a vertex table's key is, it must be **unique**. `ergon.workflow` originally keyed on `ergon.jobs (id)`, which is not unique under a temporal primary key, and every historical version of a job became its own vertex, which quietly broke every readiness query. Point the graph at a view filtered to live rows when the underlying table is bi-temporal.

### Partitioned tables

```erlang
ergon_migration:partitioned_table_sql(~"telemetry", ~"recorded_at"),
ergon_migration:partition_lifecycle_sql(~"telemetry")
```

The first emits `auto_manage_partitions_telemetry(months_ahead int DEFAULT 2)`, which creates any missing monthly partitions named `telemetry_YYYYMM`, plus one call to establish the initial horizon. The second schedules a weekly cron job to keep that horizon ahead of ingestion.

**The parent table is not created for you.** `CREATE TABLE ... PARTITION BY RANGE` is yours, because the column list is.

Three things call the emitted function, which is why it exists rather than being inlined: the initial call, the weekly cron job, and [`ergon_partition_boot_check`](operations.md#partition-safety-at-boot).
