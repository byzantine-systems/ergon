-module(ergon_test_db).
-moduledoc """
The test fixture: a per-case transaction that is rolled back, plus the escape
hatches for the cases that cannot use one.

## The sandbox

`sandbox/0` checks a connection out of Ergon's pool, issues `BEGIN`, and binds it
in the calling process's dictionary under the same key `pgo` uses for a
transaction connection. Every later `ergon_repo:query/2,3` in that process, and
therefore every `ergon_sql`, `ergon_db`, `ergon_pgmq` and `ergon_health` call,
rides that one connection. `rollback/0` undoes the lot.

Common Test runs `init_per_testcase/2`, the case body, and `end_per_testcase/2`
on the **same process**, which is what makes a process-dictionary fixture work at
all. The exception is a timetrap timeout, where the case process is killed and
`end_per_testcase/2` runs on a fresh one. That case is safe without any help
from here: the checked-out connection lives in an ETS table owned by the case
process, and when that process dies the table returns to the pool by inheritance,
where `pgo_pool` treats an unexpected transfer as `client_disconnected` and drops
the connection rather than returning it. An open transaction can therefore never
leak into a later test.

## What the sandbox cannot cover

Two things, and both are properties of the fixture rather than shortcomings to
work around:

1. **Anything running in another process.** A worker, a pgmq consumer, or
   `ergon_partition_boot_check` queries from a process of its own, which has no
   binding and so takes a fresh connection outside the transaction. There is no
   `pgo` equivalent of Ecto's `Sandbox.allow`. Those suites run committed and
   clean up after themselves, see `cleanup_jobs/1`.

2. **`NOTIFY`.** It is delivered on commit, so a notification emitted inside a
   transaction that is rolled back is never delivered to anyone. The notifier
   suite runs committed for exactly this reason.

## Naming

`unique/1` suffixes a per-run counter. Queue names, table names and pgmq queues
are cluster-wide even when the rows they hold are not, so two cases creating
`pgmq_test` would contend on the same pgmq metadata row despite the sandbox.

## Setup

`setup/0` is what every database suite calls from `init_per_suite/1`. It creates
and migrates the test database and starts the application, once per node however
many suites call it. It is deliberately **not** a CT hook: a hook runs before the
first suite whatever that suite is, which would make `ergon_fsm_SUITE` and
`ergon_temporal_period_SUITE` need a database neither one touches.
""".

-export([
    setup/0,
    database/0,
    sandbox/0,
    rollback/0,
    unique/1,
    query/1,
    query/2,
    scalar/1,
    scalar/2,
    cleanup_jobs/1,
    with_committed_conn/1
]).

%% pgo's own key for a bound connection. Reproduced here rather than reached for
%% through pgo:with_conn/2, which takes a fun and so cannot span the gap between
%% init_per_testcase/2 and end_per_testcase/2.
-define(PGO_KEY, pgo_transaction_connection).
-define(SANDBOX_KEY, ergon_test_sandbox).
-define(SETUP_KEY, {?MODULE, setup}).
-define(DEFAULT_DATABASE, "ergon_test").

-doc """
Create the test database if needed, migrate it, and start Ergon. Idempotent.

## Why a database of its own

The suite runs against `ergon_test` rather than the `ergon` development
database. Three reasons, in ascending order of importance:

1. The committed suites write rows that no transaction rolls back, and leaving
   those in a developer's database is rude.

2. `ergon_migrate:teardown/0` exists and is destructive, and something has to be
   safe to point it at.

3. **pg_cron can only be installed in one database per cluster**, named by
   `cron.database_name`, which `flake.nix` points at `ergon`. So `ergon_test` has
   no pg_cron, and the guard in `priv/migrations/bootstrap` skips it there. That
   is not a limitation being worked around: it is the contract that lets one
   migration set run in both places, and running the suite against a database
   without pg_cron is what tests it.

Override the name with `ERGON_TEST_DATABASE`.

## Why the notifier is off

`ergon_job_notifier` is disabled for the run. A notification is delivered on
commit, so nothing emitted inside a rolled-back fixture transaction ever reaches
a listener, and a notifier subscribed for the whole run would only ever see
traffic from the committed suites, which is other tests' work.
`ergon_job_notifier_SUITE` starts its own and drives it directly.
""".
-spec setup() -> ok.
setup() ->
    case persistent_term:get(?SETUP_KEY, undefined) of
        ok ->
            ok;
        undefined ->
            Database = database(),
            ok = ensure_database(Database),

            %% Before anything reads it: ergon_migrate:connect/0 and
            %% ergon_repo:pool_config/0 both resolve the database from the
            %% environment at call time, which is what lets one variable
            %% redirect the whole run.
            true = os:putenv("PGDATABASE", Database),

            ok = application:load(ergon),
            ok = application:set_env(ergon, ergon_job_notifier, [{enabled, false}]),

            {ok, _} = ergon_migrate:migrate(),
            {ok, _} = application:ensure_all_started(ergon),

            persistent_term:put(?SETUP_KEY, ok)
    end.

-doc "The database the suite runs against.".
-spec database() -> string().
database() -> os:getenv("ERGON_TEST_DATABASE", ?DEFAULT_DATABASE).

-doc """
Begin the fixture transaction, binding its connection to the calling process.
""".
-spec sandbox() -> ok.
sandbox() ->
    {ok, Ref, Conn} = pgo:checkout(ergon_repo:pool()),
    undefined = put(?PGO_KEY, Conn),
    #{command := 'begin'} = pgo:query("BEGIN", []),
    undefined = put(?SANDBOX_KEY, {Ref, Conn}),
    ok.

-doc """
Roll the fixture transaction back and return its connection to the pool.

Tolerates never having been started, so a suite may call it unconditionally.
""".
-spec rollback() -> ok.
rollback() ->
    case erase(?SANDBOX_KEY) of
        undefined ->
            ok;
        {Ref, Conn} ->
            _ = pgo:query("ROLLBACK", []),
            erase(?PGO_KEY),
            pgo:checkin(Ref, Conn)
    end.

-doc """
`Prefix` with a counter appended, unique for the lifetime of the node.

Valid as a bare SQL identifier, so it can name a table or a pgmq queue.
""".
-spec unique(binary()) -> binary().
unique(Prefix) ->
    N = erlang:unique_integer([positive, monotonic]),
    <<Prefix/binary, "_", (integer_to_binary(N))/binary>>.

-doc "Run `SQL` on whatever connection the calling process is bound to.".
-spec query(iodata()) -> map().
query(SQL) -> query(SQL, []).

-spec query(iodata(), [term()]) -> map().
query(SQL, Params) ->
    {ok, Result} = ergon_repo:query(SQL, Params),
    Result.

-doc "The single column of the single row `SQL` returns.".
-spec scalar(iodata()) -> term().
scalar(SQL) -> scalar(SQL, []).

-spec scalar(iodata(), [term()]) -> term().
scalar(SQL, Params) ->
    #{rows := [{Value}]} = query(SQL, Params),
    Value.

-doc """
Delete every job whose queue starts with `Prefix`, for the committed suites.

Both tables, and in that order. `DELETE` fires `ergon.temporal_versioning()`,
which archives the deleted row into `ergon.jobs_history`, so deleting the live
rows first and the history rows second is the only order that leaves nothing
behind. `job_edges` goes with the jobs through its `ON DELETE CASCADE`.
""".
-spec cleanup_jobs(binary()) -> ok.
cleanup_jobs(Prefix) ->
    Pattern = <<Prefix/binary, "%">>,
    _ = ergon_repo:query("DELETE FROM ergon.jobs WHERE queue LIKE $1", [Pattern]),
    _ = ergon_repo:query("DELETE FROM ergon.jobs_history WHERE queue LIKE $1", [Pattern]),
    ok.

-doc """
Run `Fun` with a connection bound but **no** transaction, so its writes commit.

For the committed suites, where a test-process write has to be visible to a
worker running in another process. Pinning the connection is still worth doing:
it keeps a case's statements in issue order against one backend rather than
scattered across the pool.
""".
-spec with_committed_conn(fun(() -> Result)) -> Result.
with_committed_conn(Fun) ->
    {ok, Ref, Conn} = pgo:checkout(ergon_repo:pool()),
    try
        pgo:with_conn(Conn, Fun)
    after
        pgo:checkin(Ref, Conn)
    end.

%% ---------------
%% Setup helpers
%% ---------------

%% CREATE DATABASE cannot run inside a transaction and cannot name the database
%% it is creating, so this connects to the cluster's maintenance database
%% instead. `ergon` is a superuser in the devenv, per flake.nix.
ensure_database(Database) ->
    {ok, _} = application:ensure_all_started(epgsql),
    {ok, Conn} = epgsql:connect(#{
        host => os:getenv("PGHOST", "127.0.0.1"),
        port => list_to_integer(os:getenv("PGPORT", "5432")),
        username => os:getenv("PGUSER", "ergon"),
        password => os:getenv("PGPASSWORD", "ergon"),
        database => "postgres",
        timeout => 10000
    }),
    try
        case epgsql:equery(Conn, "SELECT 1 FROM pg_database WHERE datname = $1", [Database]) of
            {ok, _, [_ | _]} ->
                ok;
            {ok, _, []} ->
                %% Racing another runner on the same cluster is the one failure
                %% worth absorbing: both see no database, both create it, one
                %% loses. The loser's database exists either way.
                case epgsql:squery(Conn, ["CREATE DATABASE ", Database]) of
                    {ok, [], []} -> ok;
                    {error, #{codename := duplicate_database}} -> ok
                end
        end
    after
        epgsql:close(Conn)
    end.
