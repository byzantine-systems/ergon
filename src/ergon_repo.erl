-module(ergon_repo).
-moduledoc """
Ergon's connection layer: a thin wrapper over `pgo`.

Ergon's use of the driver is deliberately shallow: the schema is installed by
`ergon_migrate`, and every statement is raw SQL loaded from `priv/queries/`
through `ergon_sql`. Nothing in Ergon assembles SQL at runtime.

## Who owns the pool

`pgo:start_pool/2` hands the pool to `pgo_sup`, a `simple_one_for_one` that
already restarts it, and returns a pid nothing in Ergon is linked to. So the pool
is **not** an `ergon_sup` child: `ergon_app:start/2` calls `start_pool/0` before
starting the tree, and `pgo` supervises the pool from there. Turning this into a
supervisor child spec would mean returning an unlinked pid to a supervisor, which
is a lie that surfaces later as a tree that will not shut down cleanly.

`start_pool/0` is idempotent so a repeated call (tests, a manual restart) is a
no-op rather than an error.

## Pool name

The pool is named `ergon`, not `pgo`'s `default`, so starting Ergon inside a host
application never clobbers that host's own default pool. Every query here injects
`pool => pool()`, so callers never have to remember it.

## Result shape

`pgo:query/3` answers a bare map on success and `{error, _}` on failure.
`query/2,3` normalises the success case to `{ok, Map}` so callers can pattern
match uniformly, which is what lets `ergon_db` express each operation as a single
`maybe` chain instead of nested cases.
""".

-export([
    pool/0,
    pool_config/0,
    start_pool/0, start_pool/2,
    query/2, query/3,
    transaction/1, transaction/2
]).

-include_lib("ergon/include/ergon.hrl").

-export_type([repo_result/0, query_options/0]).

-define(DEFAULT_POOL, ergon).
-define(DEFAULT_POOL_SIZE, 10).

%% ---------------
%% Configuration
%% ---------------

-doc """
The name of the `pgo` pool Ergon runs on.

Defaults to `ergon`. Override with `{ergon, [{ergon_repo, [{pool, my_pool}]}]}`
to share a pool a host application already started.
""".
-spec pool() -> atom().
pool() -> env(pool, ?DEFAULT_POOL).

-doc """
The `pgo` pool configuration, resolved from the `PG*` environment at call time.

Read from the environment rather than baked into `sys.config` so credentials stay
out of the repository and a release picks them up at boot. Same variables and same
defaults as `ergon_migrate:connect/0`, which is the other half of this contract:
the boot-time migration connection and the runtime pool must reach the same
database.
""".
-spec pool_config() -> pgo:pool_config().
pool_config() ->
    #{
        host => os:getenv("PGHOST", "127.0.0.1"),
        port => list_to_integer(os:getenv("PGPORT", "5432")),
        user => os:getenv("PGUSER", "ergon"),
        password => os:getenv("PGPASSWORD", "ergon"),
        database => os:getenv("PGDATABASE", "ergon"),
        pool_size => env(pool_size, ?DEFAULT_POOL_SIZE)
    }.

%% ---------------
%% Lifecycle
%% ---------------

-doc """
Start the pool under `pgo_sup`. Idempotent.

Called by `ergon_app:start/2` before the supervision tree, so `ergon_sql` and
every later consumer can assume the pool exists.

The pool registers itself locally under its name, so an already-running pool is
detected with `whereis/1` rather than by starting a second one and inspecting the
`{already_started, _}` error. That is not only tidier: `pgo_sup` is a
`simple_one_for_one`, so a duplicate start really does spawn a child that then
fails to register, and the failure surfaces as a supervisor report either way.
The `whereis/1` check races only if two processes call this concurrently, which
boot does not.
""".
-spec start_pool() -> {ok, pid()}.
start_pool() -> start_pool(pool(), #{}).

-doc """
Start an additional named pool, with `Overrides` merged over the usual `PG*`
configuration.

Used for connections that must not come out of the shared pool. A long-polling
pgmq consumer takes one of these sized `1`, because its read blocks server-side
and would otherwise hold a shared connection idle for seconds at a time.
""".
-spec start_pool(atom(), map()) -> {ok, pid()}.
start_pool(Pool, Overrides) when is_atom(Pool), is_map(Overrides) ->
    case whereis(Pool) of
        undefined -> pgo:start_pool(Pool, maps:merge(pool_config(), Overrides));
        Pid when is_pid(Pid) -> {ok, Pid}
    end.

%% ---------------
%% Querying
%% ---------------

-doc "Run `SQL` with positional `Params` on Ergon's pool.".
-spec query(iodata(), [term()]) -> repo_result().
query(SQL, Params) -> query(SQL, Params, #{}).

-doc """
Like `query/2` with extra `pgo` options.

The options map is threaded all the way through from `ergon_sql:query/3`, which
is what lets a test pin every statement in a case to one checked-out connection,
and what lets a long-polling pgmq consumer read on a pool of its own.

`pool` is defaulted here rather than forced, so a caller that names one wins. One
caveat on that: inside `transaction/1,2` the query must stay on the transaction's
pool, and `pgo` raises `{in_other_pool_transaction, _}` if it does not. Naming a
different pool is therefore only meaningful outside a transaction.
""".
-spec query(iodata(), [term()], query_options()) -> repo_result().
query(SQL, Params, Opts) ->
    case pgo:query(SQL, Params, maps:merge(#{pool => pool()}, Opts)) of
        #{rows := _} = Result -> {ok, Result};
        {error, _} = Error -> Error
    end.

-doc "Run `Fun` inside a SQL transaction on Ergon's pool.".
-spec transaction(fun(() -> Result)) -> Result | {error, term()}.
transaction(Fun) -> transaction(Fun, #{}).

-doc """
Like `transaction/1` with extra `pgo` options.

`pgo` binds the transaction's connection in the process dictionary, so every
nested `query/2,3` in `Fun` automatically rides it, with no connection threading.

Two things to know about the commit rule. `pgo` commits unless `Fun` **raises**:
returning `{error, _}` still commits. That is correct where the error path wrote
nothing (`ergon_db:link/3` rejecting a cycle commits an empty transaction), and
harmless where a statement itself failed, because PostgreSQL has already aborted
the transaction and the COMMIT comes back as a rollback. To force a rollback from
a successful-looking path, throw.
""".
-spec transaction(fun(() -> Result), query_options()) -> Result | {error, term()}.
transaction(Fun, Opts) ->
    pgo:transaction(Fun, Opts#{pool => pool()}).

%% ---------------
%% Helpers
%% ---------------

env(Key, Default) ->
    proplists:get_value(Key, application:get_env(ergon, ?MODULE, []), Default).
