-module(ergon_migrate).
-moduledoc """
Ergon's schema migrations, driven by `migraterl`.

The whole schema lives in ordinary `.sql` files under `priv/migrations/`, applied
in source order:

```text
bootstrap/  once       extensions, CREATE SCHEMA ergon
functions/  on_change  routines the schema depends on (attached as triggers)
schema/     once       the tables, indexes, constraints, RLS, property graph
routines/   on_change  routines that depend on the schema
cron/       always     the pg_cron notifier tick
teardown/   -          NOT a source; run by teardown/1
```

Order is necessary in both directions. `functions/` must precede `schema/`
because the four triggers there reference `ergon.temporal_versioning()`,
`ergon.enforce_job_transition()`, `ergon.block_child()`, and
`ergon.unblock_children()`. Those bodies name `ergon.jobs` themselves, which is
fine only because PL/pgSQL defers name resolution to run time; a `LANGUAGE sql`
body would be rejected before the table existed. `routines/` must follow
`schema/` for exactly that reason: `ergon.enqueue` and `ergon.jobs_asof*` name
`ergon.jobs` as a return type, and `ergon.notify_pending_jobs` has a
`LANGUAGE sql` body that PostgreSQL validates at `CREATE` time.

`migraterl` scans each source directory non-recursively, sorts its `.sql` files
lexically, and journals each one under its **basename**. Names must therefore be
unique across every source in the namespace, which is what the single
`000001..000015` sequence is for: it fixes the order within a directory and
keeps the journal keys distinct across directories.

## Classes

`once` scripts are applied one time, keyed by name. `on_change` scripts are
re-applied whenever their content hash changes, which is why every routine is
`CREATE OR REPLACE`: editing a function body is an edit to its own file rather
than a new migration. `always` scripts run on every pass and are never journaled.

## Dollar quoting

`migraterl` hands each script whole to `epgsql:squery/2` (the simple query
protocol) and lets PostgreSQL's own parser split the statements, so `$$`-quoted
PL/pgSQL bodies are fine in a plain `.sql` file. A runner that split on `;`
itself could not tolerate them.

One caveat: `migraterl` substitutes `$key$` tokens from its
`variables` option before executing, so a variable named `foo` would rewrite the
dollar-quote tag `$foo$`. `opts/1` passes no variables and every body in
`priv/migrations` uses a bare `$$`.
""".

-export([
    namespace/0,
    namespaces/0,
    sources/0,
    sources/1,
    opts/0,
    opts/1,
    connect/0,
    disconnect/1,
    plan/0, plan/1,
    migrate/0, migrate/1,
    status/0, status/1,
    teardown/0, teardown/1
]).

-define(NAMESPACE, <<"ergon">>).

%% Sources in application order, paired with their migraterl class. `teardown`
%% is deliberately absent: migraterl is forward-only and would apply drop.sql
%% as just another script.
-define(SOURCES, [
    {once, "bootstrap"},
    {on_change, "functions"},
    {once, "schema"},
    {on_change, "routines"},
    {always, "cron"}
]).

-type summary() :: migraterl:summary().
-export_type([summary/0]).

%% ---------------
%% Configuration
%% ---------------

-doc "The migraterl namespace Ergon journals its scripts under.".
-spec namespace() -> binary().
namespace() -> ?NAMESPACE.

-doc "Ordered `{Class, Directory}` sources, resolved against ergon's priv dir.".
-spec sources() -> [{once | on_change | always, file:filename_all()}].
sources() -> sources(priv_dir()).

-doc "Like `sources/0`, but rooted at an explicit priv directory.".
-spec sources(file:filename_all()) -> [{once | on_change | always, file:filename_all()}].
sources(PrivDir) ->
    [{Class, filename:join([PrivDir, "migrations", Dir])} || {Class, Dir} <- ?SOURCES].

-doc """
Every namespace this runner applies, Ergon's first.

A host registers its own migration directories through application environment,
each under **its own migraterl namespace**:

```erlang
{ergon, [{ergon_migrate, [
    {extra_sources, [
        #{namespace => ~"my_app",
          sources => [{once, {priv, my_app, "migrations"}}]}
    ]}
]}]}
```

The separate namespace is what makes this safe rather than merely tidy.
migraterl journals every script under its **basename** and checks ordering
against that journal, so sharing Ergon's namespace would put host filenames in
competition with `000001..000015` for sort position, and a basename that happened
to collide would mark a host script as already applied without ever running it.
Namespaces are independent journals with their own advisory lock, so neither can
happen and a host is free to number its scripts however it likes.

Source directories accept the forms `ergon_path:resolve_root/1` understands, the
same ones `ergon_sql`'s `extra_roots` takes.
""".
-spec namespaces() -> [#{namespace := binary(), opts := map()}].
namespaces() ->
    Ergon = #{namespace => ?NAMESPACE, opts => opts()},
    [Ergon | [host_namespace(Spec) || Spec <- extra_sources()]].

host_namespace(#{namespace := Namespace, sources := Sources} = Spec) ->
    Opts = maps:merge(
        #{
            namespace => Namespace,
            sources => [{Class, ergon_path:resolve_root(Root)} || {Class, Root} <- Sources],
            txn => per_script,
            on_out_of_order => error,
            variables => #{}
        },
        maps:get(opts, Spec, #{})
    ),
    #{namespace => Namespace, opts => Opts}.

extra_sources() ->
    proplists:get_value(extra_sources, application:get_env(ergon, ?MODULE, []), []).

-doc "The options map passed to `migraterl:migrate/2` for Ergon's own namespace.".
-spec opts() -> map().
opts() -> opts(#{}).

-doc """
Like `opts/0` with per-key overrides, e.g. `opts(#{txn => single})`.

Defaults to `txn => per_script`, so a failing script rolls back on its own and
the scripts before it stay applied. `on_out_of_order => error` rather than
migraterl's `warn` default: a not-yet-applied `once` script sorting before one
already applied means the numbering was reused, and that should stop the run
rather than log.
""".
-spec opts(map()) -> map().
opts(Overrides) ->
    maps:merge(
        #{
            namespace => ?NAMESPACE,
            sources => sources(),
            txn => per_script,
            on_out_of_order => error,
            %% Intentionally empty: see the dollar-quoting note in the module doc.
            variables => #{}
        },
        Overrides
    ).

%% ---------------
%% Connection
%% ---------------

-doc """
Open a boot-time `epgsql` connection from the `PG*` environment variables.

`migraterl` speaks `epgsql`, not `pgo`, so this is the one place Ergon opens an
epgsql connection at all. It is short-lived: open it, migrate, `disconnect/1`.
Everything at runtime goes through `ergon_repo` on the pgo pool.
""".
-spec connect() -> {ok, epgsql:connection()} | {error, term()}.
connect() ->
    {ok, _} = application:ensure_all_started(epgsql),
    Config = #{
        host => os:getenv("PGHOST", "127.0.0.1"),
        port => list_to_integer(os:getenv("PGPORT", "5432")),
        username => os:getenv("PGUSER", "ergon"),
        password => os:getenv("PGPASSWORD", "ergon"),
        database => os:getenv("PGDATABASE", "ergon"),
        timeout => 10000
    },
    epgsql:connect(Config).

-spec disconnect(epgsql:connection()) -> ok.
disconnect(Conn) -> epgsql:close(Conn).

%% ---------------
%% Operations
%% ---------------

-doc """
Dry run: report what `migrate/1` would apply, without applying it.

Answers one summary per namespace, Ergon's first, in the order `migrate/1` would
apply them.
""".
-spec plan() -> {ok, [{binary(), summary()}]} | {error, term()}.
plan() -> with_conn(fun plan/1).

-spec plan(epgsql:connection()) -> {ok, [{binary(), summary()}]} | {error, term()}.
plan(Conn) -> per_namespace(Conn, fun migraterl:plan/2).

-doc """
Apply every pending script, in every namespace.

Ergon's namespace goes first and host namespaces follow in the order they were
registered. That ordering is not incidental: a host table that references
`ergon.jobs`, or attaches `ergon.temporal_versioning()` as a trigger, needs those
to exist already.
""".
-spec migrate() -> {ok, [{binary(), summary()}]} | {error, term()}.
migrate() -> with_conn(fun migrate/1).

-spec migrate(epgsql:connection()) -> {ok, [{binary(), summary()}]} | {error, term()}.
migrate(Conn) -> per_namespace(Conn, fun migraterl:migrate/2).

-doc "The currently-applied journal state, per namespace.".
-spec status() -> {ok, [{binary(), [map()]}]} | {error, term()}.
status() -> with_conn(fun status/1).

-spec status(epgsql:connection()) -> {ok, [{binary(), [map()]}]} | {error, term()}.
status(Conn) ->
    fold_namespaces(
        [{Namespace, undefined} || #{namespace := Namespace} <- namespaces()],
        fun(Namespace, _Opts) -> migraterl:status(Conn, Namespace) end
    ).

%% Run Fun once per namespace, stopping at the first failure and collecting the
%% results in application order.
per_namespace(Conn, Fun) ->
    fold_namespaces(
        [{Namespace, Opts} || #{namespace := Namespace, opts := Opts} <- namespaces()],
        fun(_Namespace, Opts) -> Fun(Conn, Opts) end
    ).

fold_namespaces(Entries, Fun) ->
    fold_namespaces(Entries, Fun, []).

fold_namespaces([], _Fun, Acc) ->
    {ok, lists:reverse(Acc)};
fold_namespaces([{Namespace, Opts} | Rest], Fun, Acc) ->
    case Fun(Namespace, Opts) of
        {ok, Result} -> fold_namespaces(Rest, Fun, [{Namespace, Result} | Acc]);
        {error, Reason} -> {error, {Namespace, Reason}}
    end.

-doc """
Drop everything **Ergon's** migration set created, and forget it was applied.

Runs `priv/migrations/teardown/drop.sql`, then deletes the `ergon` namespace's
rows from `migraterl.schema_journal` so a subsequent `migrate/1` replays from
scratch. Without the journal wipe the `once` scripts would be skipped and the
schema would never come back.

**Host namespaces are deliberately left alone**, unlike `migrate/1` which applies
them all. A host's schema is not Ergon's to drop, and guessing at how to unwind
it would be worse than not trying: there is no teardown script to run and no way
to know what depends on those tables. A host that wants a full reset tears its
own schema down first, then calls this.

Two ways a host table gets dropped anyway, both worth knowing because both look
like this function misbehaving:

1. **It is in the `ergon` schema without meaning to be.** Ergon connects as a role
   named `ergon`, and the default `search_path` of `"$user", public` resolves
   `$user` to the `ergon` schema. So an unqualified `CREATE TABLE` in a host
   migration lands inside Ergon's schema and goes with it. Qualify host DDL, or
   set a `search_path` on the migration.
2. **It depends on an Ergon type.** A column typed `ergon.job_state`, or a
   foreign key into an Ergon table, makes the host table a dependent of the
   schema, and `DROP SCHEMA ergon CASCADE` takes dependents with it. That is
   correct cascade behaviour rather than something this function chooses.

Destructive. The dev and test reset path, not something to run against a database
holding jobs.
""".
-spec teardown() -> ok | {error, term()}.
teardown() -> with_conn(fun teardown/1).

-spec teardown(epgsql:connection()) -> ok | {error, term()}.
teardown(Conn) ->
    Path = filename:join([priv_dir(), "migrations", "teardown", "drop.sql"]),
    case file:read_file(Path) of
        {ok, SQL} ->
            case squery(Conn, SQL) of
                ok -> forget(Conn);
                {error, _} = Err -> Err
            end;
        {error, Reason} ->
            {error, {read_error, Path, Reason}}
    end.

%% Clear the namespace's journal rows. Tolerates a missing journal: tearing
%% down a database that was never migrated is a no-op, not an error.
forget(Conn) ->
    Delete = "DELETE FROM migraterl.schema_journal WHERE namespace = $1",
    case epgsql:equery(Conn, Delete, [?NAMESPACE]) of
        {ok, _} -> ok;
        {error, {error, error, <<"42P01">>, _, _, _}} -> ok;
        {error, _} = Err -> Err;
        Other -> {error, {journal_cleanup_failed, Other}}
    end.

%% ---------------
%% Helpers
%% ---------------

with_conn(Fun) ->
    case connect() of
        {ok, Conn} ->
            try
                Fun(Conn)
            after
                disconnect(Conn)
            end;
        {error, _} = Err ->
            Err
    end.

%% Run a possibly-multi-statement script over the simple query protocol and
%% collapse the per-statement results into ok | {error, Reason}.
squery(Conn, SQL) ->
    Results =
        case epgsql:squery(Conn, SQL) of
            R when is_list(R) -> R;
            R -> [R]
        end,
    case lists:keyfind(error, 1, Results) of
        false -> ok;
        {error, Reason} -> {error, Reason}
    end.

priv_dir() ->
    case code:priv_dir(ergon) of
        {error, bad_name} -> erlang:error({priv_dir_not_found, ergon});
        Dir -> Dir
    end.
