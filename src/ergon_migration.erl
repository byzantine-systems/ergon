-module(ergon_migration).
-moduledoc """
DDL generators for host tables that want Ergon's patterns.

Ergon's own schema is not built with these. It lives in `priv/migrations/` and is
applied by `ergon_migrate`. This module is for the *host*: the same bi-temporal
shape `ergon.jobs` uses, the same property graph element tables, the same monthly
partition lifecycle, generated for tables Ergon knows nothing about.

Every function returns SQL as iodata and executes nothing. Put the output in a
`.sql` file, register the directory with `ergon_migrate`, and it is applied and
journalled alongside Ergon's own:

```erlang
{ergon, [{ergon_migrate, [
    {extra_sources, [
        #{namespace => ~"my_app",
          sources => [{once, {priv, my_app, "migrations"}}]}
    ]}
]}]}
```

`script/1` joins a statement list into something writable to such a file.

## What used to be here

Roughly half of the original helper set has been absorbed elsewhere, and the
short answer to "where did X go" is that Ergon now installs its own schema:

- extension installation, now `priv/migrations/bootstrap`.
- the `temporal_versioning()` function itself, now
  `priv/migrations/functions`. It is column-agnostic by design, so a host needs
  only the trigger, which is `versioning_trigger_sql/1`.
- pgmq queue creation and notification, now `ergon_pgmq:create_queue_sql/1` and
  `ergon_pgmq:enable_notify_sql/1,2`.
- the job notifier tick, now `priv/migrations/cron`, since it is Ergon's own.

## A note on schemas

These generate unqualified names, so the objects land wherever the host's
`search_path` points, which is the host's business. The one qualified reference
is `ergon.temporal_versioning()`, deliberately: it belongs to Ergon and naming it
bare would resolve against the caller's `search_path`, which is exactly the bug
that put every Ergon routine in the wrong schema before Phase 2.
""".

-export([
    bitemporal_table_sql/2,
    versioning_trigger_sql/1,
    history_twin_sql/1,
    vertex_table_sql/1, vertex_table_sql/2,
    edge_table_sql/3, edge_table_sql/4,
    partitioned_table_sql/2,
    partition_lifecycle_sql/1,
    script/1
]).

-type endpoint() :: {Column :: binary(), Table :: binary()}.
-type vertex_opts() :: #{
    references => endpoint(),
    extra_columns => iodata()
}.
-type edge_opts() :: #{
    check => iodata(),
    cascade_source => boolean()
}.
-export_type([endpoint/0, vertex_opts/0, edge_opts/0]).

-define(DEFAULT_MONTHS_AHEAD, 2).

%% ---------------
%% Bi-temporal tables
%% ---------------

-doc """
The full bi-temporal table shape: table, history twin, time-travel index, and
versioning trigger.

`DataColumns` is a raw SQL fragment for the host's own columns, inserted between
the generated `id` and the two period columns. It is passed through verbatim, so
it is the one argument here that is not validated: a host writing its own column
definitions is writing SQL either way.

Two periods, as in `ergon.jobs`. `valid_time` is application time, when the row
is true in the world, split by `UPDATE ... FOR PORTION OF`. `system_time` is
belief time, maintained by the trigger, with superseded rows archived into the
history twin. The primary key is temporal: an id is unique at any instant, but
the same id may own many non-overlapping historical versions.

```erlang
ergon_migration:bitemporal_table_sql(~"assets", ~"name text NOT NULL, tag text")
```
""".
-spec bitemporal_table_sql(binary(), iodata()) -> [iodata()].
bitemporal_table_sql(Table, DataColumns) ->
    Name = ergon_ident:validate(Table, table),
    Seq = [Name, "_id_seq"],
    [
        ["CREATE SEQUENCE ", Seq],
        [
            "CREATE TABLE ",
            Name,
            " (\n"
            "    id bigint NOT NULL DEFAULT nextval('",
            Seq,
            "'),\n    ",
            DataColumns,
            ",\n"
            "    valid_time tstzrange NOT NULL DEFAULT tstzrange(now(), 'infinity', '[)'),\n"
            "    system_time tstzrange NOT NULL DEFAULT tstzrange(statement_timestamp(), 'infinity', '[)'),\n"
            "    PRIMARY KEY (id, valid_time WITHOUT OVERLAPS)\n)"
        ],
        %% Owned by the table so dropping the table drops the sequence.
        ["ALTER SEQUENCE ", Seq, " OWNED BY ", Name, ".id"]
        | history_twin_sql(Name) ++ [versioning_trigger_sql(Name)]
    ].

-doc """
The history twin and its time-travel index for an existing table.

`LIKE ... INCLUDING DEFAULTS INCLUDING CONSTRAINTS` copies columns, CHECKs and
NOT NULLs but deliberately **not** indexes or generated columns. History is an
append-only log the trigger writes verbatim with `INSERT ... SELECT (old).*`, so
a generated column there would refuse the write and a copied temporal PK would
reject the very overlaps history exists to record.
""".
-spec history_twin_sql(binary()) -> [iodata()].
history_twin_sql(Table) ->
    Name = ergon_ident:validate(Table, table),
    History = [Name, "_history"],
    [
        ["CREATE TABLE ", History, " (LIKE ", Name, " INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"],
        [
            "CREATE INDEX ",
            History,
            "_id_system_time_idx ON ",
            History,
            " USING gist (id, system_time)"
        ]
    ].

-doc """
Attach Ergon's system-time versioning trigger to a table.

The function it attaches, `ergon.temporal_versioning()`, is installed by Ergon's
own migrations and inspects `TG_TABLE_NAME` at fire time to find the history twin
by naming convention, so it serves any table with a `<table>_history` twin and a
`system_time` column. A host defines no function of its own.

Qualified as `ergon.` on purpose: a bare name would resolve against the caller's
`search_path`.
""".
-spec versioning_trigger_sql(binary()) -> iodata().
versioning_trigger_sql(Table) ->
    Name = ergon_ident:validate(Table, table),
    [
        "CREATE TRIGGER ",
        Name,
        "_versioning_trigger\n"
        "    BEFORE INSERT OR UPDATE OR DELETE ON ",
        Name,
        "\n    FOR EACH ROW EXECUTE FUNCTION ergon.temporal_versioning()"
    ].

%% ---------------
%% Property graph element tables
%% ---------------

-doc "A vertex table with a generated identity.".
-spec vertex_table_sql(binary()) -> [iodata()].
vertex_table_sql(Table) -> vertex_table_sql(Table, #{}).

-doc """
A vertex table for a property graph.

A vertex table is an identity registry, one row per logical entity, kept in step
with a domain table by a trigger the host writes. Ergon emits the shape but not
that trigger, because what counts as a new vertex is domain-specific.

Options:

- `references => {Column, ParentTable}` makes `id` a foreign key into a domain
  table with `ON DELETE CASCADE`. Omit it when the vertex table owns its own
  identity, which is also the only choice when the parent's primary key is
  temporal and therefore not referenceable.
- `extra_columns => SQL` for additional columns.

One lesson from Ergon's own graph is worth repeating here: whatever a vertex
table's key is, it must be **unique**. `ergon.workflow` originally keyed on
`ergon.jobs (id)`, which is not unique under a temporal primary key, and every
historical version of a job became its own vertex. Point the graph at a view
filtered to live rows if the underlying table is bi-temporal.
""".
-spec vertex_table_sql(binary(), vertex_opts()) -> [iodata()].
vertex_table_sql(Table, Opts) ->
    Name = ergon_ident:validate(Table, table),
    Id =
        case maps:find(references, Opts) of
            {ok, {Column, Parent}} ->
                [
                    "id bigint PRIMARY KEY REFERENCES ",
                    ergon_ident:validate(Parent, table),
                    " (",
                    ergon_ident:validate(Column, column),
                    ") ON DELETE CASCADE"
                ];
            error ->
                "id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"
        end,
    Columns =
        case maps:get(extra_columns, Opts, <<>>) of
            <<>> -> [Id];
            Extra -> [Id, ", ", Extra]
        end,
    [["CREATE TABLE ", Name, " (", Columns, ")"]].

-doc "A bi-temporal edge table between two vertex tables.".
-spec edge_table_sql(binary(), endpoint(), endpoint()) -> [iodata()].
edge_table_sql(Table, Source, Dest) -> edge_table_sql(Table, Source, Dest, #{}).

-doc """
A bi-temporal edge table between two vertex tables, with history.

`Source` and `Dest` are `{Column, VertexTable}` pairs. The uniqueness constraint
is temporal, `UNIQUE (src, dst, valid_time WITHOUT OVERLAPS)`, so the same edge
may exist, end, and exist again without colliding with its own history.

Options:

- `check => SQL` adds a CHECK constraint, e.g. `~"from_id <> to_id"` to ban
  self-loops.
- `cascade_source => true` cascades deletes from the source too. Only the
  destination cascades by default, on the reasoning that removing a thing should
  remove the edges pointing *at* it, while edges pointing *from* it are usually
  worth keeping until deliberately cleared.
""".
-spec edge_table_sql(binary(), endpoint(), endpoint(), edge_opts()) -> [iodata()].
edge_table_sql(Table, {SrcCol, SrcTable}, {DstCol, DstTable}, Opts) ->
    Name = ergon_ident:validate(Table, table),
    Src = ergon_ident:validate(SrcCol, column),
    SrcT = ergon_ident:validate(SrcTable, table),
    Dst = ergon_ident:validate(DstCol, column),
    DstT = ergon_ident:validate(DstTable, table),
    SrcFk =
        case maps:get(cascade_source, Opts, false) of
            true -> [" REFERENCES ", SrcT, " (id) ON DELETE CASCADE"];
            false -> [" REFERENCES ", SrcT, " (id)"]
        end,
    Check =
        case maps:find(check, Opts) of
            {ok, SQL} -> [",\n    CHECK (", SQL, ")"];
            error -> []
        end,
    [
        [
            "CREATE TABLE ",
            Name,
            " (\n"
            "    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\n    ",
            Src,
            " bigint NOT NULL",
            SrcFk,
            ",\n    ",
            Dst,
            " bigint NOT NULL REFERENCES ",
            DstT,
            " (id) ON DELETE CASCADE,\n"
            "    valid_time tstzrange NOT NULL DEFAULT tstzrange(now(), 'infinity', '[)'),\n"
            "    system_time tstzrange NOT NULL DEFAULT tstzrange(statement_timestamp(), 'infinity', '[)')",
            Check,
            ",\n    UNIQUE (",
            Src,
            ", ",
            Dst,
            ", valid_time WITHOUT OVERLAPS)\n)"
        ]
        | history_twin_sql(Name) ++ [versioning_trigger_sql(Name)]
    ].

%% ---------------
%% Partition lifecycle
%% ---------------

-doc """
The monthly partition lifecycle for a RANGE-partitioned table.

Emits `auto_manage_partitions_<table>(months_ahead int DEFAULT 2)`, which creates
any missing monthly partitions named `<table>_YYYYMM` from the current month
through the horizon, plus one call to establish the initial horizon.

**The parent table is not created here.** `CREATE TABLE ... PARTITION BY RANGE`
is the host's, because the column list is. `PartitionColumn` is documentation
only: the generated body assumes monthly ranges.

Three things call the emitted function, which is why it exists rather than being
inlined: this initial call, the weekly `partition-lifecycle-<table>` cron job
from `partition_lifecycle_sql/1`, and `ergon_partition_boot_check` at boot.
""".
-spec partitioned_table_sql(binary(), binary()) -> [iodata()].
partitioned_table_sql(Table, _PartitionColumn) ->
    Name = ergon_ident:validate(Table, table),
    Fn = ["auto_manage_partitions_", Name],
    [
        [
            "CREATE OR REPLACE FUNCTION ",
            Fn,
            "(months_ahead int DEFAULT ",
            integer_to_list(?DEFAULT_MONTHS_AHEAD),
            ")\n"
            "RETURNS int\n"
            "LANGUAGE plpgsql AS $ergon_partitions$\n"
            "DECLARE\n"
            "    month_start date;\n"
            "    partition_name text;\n"
            "    created int := 0;\n"
            "BEGIN\n"
            "    FOR i IN 0..months_ahead LOOP\n"
            "        month_start := (date_trunc('month', now()) + make_interval(months => i))::date;\n"
            "        partition_name := '",
            Name,
            "_' || to_char(month_start, 'YYYYMM');\n"
            "        IF to_regclass(partition_name) IS NULL THEN\n"
            "            EXECUTE format(\n"
            "                'CREATE TABLE %I PARTITION OF ",
            Name,
            " FOR VALUES FROM (%L) TO (%L)',\n"
            "                partition_name, month_start, month_start + interval '1 month'\n"
            "            );\n"
            "            created := created + 1;\n"
            "        END IF;\n"
            "    END LOOP;\n"
            "    RETURN created;\n"
            "END\n"
            "$ergon_partitions$"
        ],
        ["SELECT ", Fn, "()"]
    ].

-doc """
The weekly cron job that keeps a partitioned table's horizon ahead of ingestion.

Separate from `partitioned_table_sql/2` because a cron schedule is not schema:
re-running it is harmless but it belongs with the host's other scheduling rather
than in a `once` migration. Guarded and idempotent through `ergon_cron`.

One job per table is correct here. See `ergon_cron` for why this is not the same
situation as the pgmq notifier.
""".
-spec partition_lifecycle_sql(binary()) -> iodata().
partition_lifecycle_sql(Table) ->
    Name = ergon_ident:validate(Table, table),
    ergon_cron:schedule_sql(
        <<"partition-lifecycle-", Name/binary>>,
        ~"@weekly",
        ["SELECT auto_manage_partitions_", Name, "()"]
    ).

%% ---------------
%% Helpers
%% ---------------

-doc """
Join generated statements into a script, ready to write to a `.sql` file.

Statement-terminated and blank-line separated, because migraterl hands a whole
file to the simple query protocol and lets PostgreSQL split it.
""".
-spec script([iodata()]) -> binary().
script(Statements) ->
    iolist_to_binary([[S, ";\n\n"] || S <- Statements]).
