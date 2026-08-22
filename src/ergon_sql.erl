-module(ergon_sql).
-moduledoc """
Filesystem-backed SQL loader and executor.

## Convention

Statements live at `priv/queries/<domain>/<operation>.sql` and use PostgreSQL
positional parameters (`$1..$n`). Each file is keyed by `{Domain, Operation}`, so
`priv/queries/jobs/insert.sql` becomes `{jobs, insert}`:

```erlang
ergon_sql:query({jobs, insert}, [Queue, Worker, Payload, MaxAttempts, Dedup])
```

This `gen_server` walks the query roots once at boot and caches every statement in
a read-optimised ETS table, so executing a named statement is a lock-free
`ets:lookup/2` plus a `ergon_repo:query/3`. It is supervised ahead of every
consumer that depends on it.

## Host query directories

Host applications register additional `priv/queries`-style directories through
application environment, so one loader serves both Ergon's statements and
host-owned domains (assets, telemetry, spatial, ...). The default is Ergon's own
`priv/queries` only:

```erlang
{ergon, [{ergon_sql, [{extra_roots, [
    {priv, my_app, "queries"},        %% <my_app>/priv/queries
    {app, my_app, "priv/queries"},    %% <my_app>/priv/queries, spelled the long way
    "/absolute/path/to/queries"       %% used verbatim
]}]}]}
```

Keys from extra roots share the `{Domain, Operation}` namespace; a collision
across roots is rejected at load time exactly like one inside a single root.

## Naming

`find/1` returns `{ok, SQL} | error`; `fetch/1` returns the statement or raises.
That is the `maps:find/2` / `maps:get/2` pairing, and the raising form carries an
EEP-54 `error_info` so a mistyped key prints the known keys and the path the file
was expected at, rather than a bare `badkey`.
""".

-behaviour(gen_server).

-export([
    start_link/0, start_link/1,
    find/1,
    fetch/1,
    keys/0,
    query/2, query/3,
    reload/0
]).

-export([format_error/2]).

-export([init/1, handle_call/3, handle_cast/2]).

-include_lib("kernel/include/logger.hrl").
-include_lib("ergon/include/ergon.hrl").

-export_type([sql_key/0, sql_root/0, sql_option/0]).

-define(TABLE, ?MODULE).

%% ---------------
%% Public API
%% ---------------

-doc "Start the loader with options taken from application environment.".
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() -> start_link([]).

-doc """
Start the loader, merging `Opts` over the application environment.

The `root` option replaces Ergon's own `priv/queries` rather than adding to it,
which is how a test suite points the loader at a fixture directory.
""".
-spec start_link([sql_option()]) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    Merged = application:get_env(ergon, ?MODULE, []) ++ Opts,
    gen_server:start_link({local, ?MODULE}, ?MODULE, Merged, []).

-doc "The raw SQL registered for `Key`, or `error` if there is none.".
-spec find(sql_key()) -> {ok, binary()} | error.
find(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, SQL}] -> {ok, SQL};
        [] -> error
    end.

-doc "Like `find/1`, but raises for an unknown `Key`.".
-spec fetch(sql_key()) -> binary().
fetch(Key) ->
    case find(Key) of
        {ok, SQL} ->
            SQL;
        error ->
            erlang:error(
                unknown_sql_key,
                [Key],
                [{error_info, #{module => ?MODULE, cause => #{key => Key}}}]
            )
    end.

-doc "Every registered `{Domain, Operation}` key, sorted.".
-spec keys() -> [sql_key()].
keys() -> lists:sort([Key || {Key, _SQL} <- ets:tab2list(?TABLE)]).

-doc "Execute the statement registered for `Key` with positional `Params`.".
-spec query(sql_key(), [term()]) -> repo_result().
query(Key, Params) -> query(Key, Params, #{}).

-doc """
Like `query/2` with extra `pgo` options, forwarded untouched to
`ergon_repo:query/3`.

Keep this pass-through: it is the only way a caller can pin a statement to a
specific connection, which both the RLS tenant path and the test fixtures need.
""".
-spec query(sql_key(), [term()], query_options()) -> repo_result().
query(Key, Params, Opts) -> ergon_repo:query(fetch(Key), Params, Opts).

-doc """
Re-read the query roots from disk. Returns the number of statements loaded.

Useful in development after editing a statement; production never needs it.
""".
-spec reload() -> non_neg_integer().
reload() -> gen_server:call(?MODULE, reload).

%% ---------------
%% gen_server
%% ---------------

-spec init([sql_option()]) -> {ok, [sql_option()]}.
init(Opts) ->
    proc_lib:set_label(?MODULE),
    ?TABLE = ets:new(?TABLE, [named_table, set, protected, {read_concurrency, true}]),
    Count = load(Opts),
    ?LOG_INFO("ergon_sql loaded ~b statement(s) from ~p", [Count, roots(Opts)]),
    {ok, Opts}.

handle_call(reload, _From, Opts) ->
    {reply, load(Opts), Opts}.

handle_cast(_Msg, Opts) ->
    {noreply, Opts}.

%% ---------------
%% Loading
%% ---------------

%% Upserts atomically instead of clear-then-rebuild. `ets:insert/2` is atomic
%% even for a whole list of objects, so a key present both before and after a
%% reload is never observably absent in between. That matters because the table
%% is a single named table shared by every reader: a `delete_all_objects`
%% followed by a separate `insert` leaves a real window in which a concurrent
%% `fetch/1` sees an empty cache and raises. Only keys whose backing file has
%% disappeared since the last load, which is the development case `reload/0`
%% exists for, are deleted, and only after the new data is in.
load(Opts) ->
    Entries = lists:append([entries(Root) || Root <- roots(Opts)]),
    ok = reject_collisions(Entries),
    Fresh = maps:from_keys([Key || {Key, _SQL} <- Entries], []),
    Stale = [Key || {Key, _SQL} <- ets:tab2list(?TABLE), not is_map_key(Key, Fresh)],
    true = ets:insert(?TABLE, Entries),
    _ = [ets:delete(?TABLE, Key) || Key <- Stale],
    length(Entries).

entries(Root) ->
    [
        {key_for(Relative, Root), read_file(filename:join(Root, Relative))}
     || Relative <- lists:sort(filelib:wildcard("**/*.sql", Root))
    ].

%% jobs/insert.sql -> {jobs, insert}
key_for(Relative, Root) ->
    Operation = filename:basename(Relative, ".sql"),
    case filename:basename(filename:dirname(Relative)) of
        Domain when Domain =:= "."; Domain =:= "" ->
            erlang:error(
                {top_level_sql_file, filename:join(Root, Relative)},
                [Relative, Root],
                [{error_info, #{module => ?MODULE}}]
            );
        Domain ->
            {list_to_atom(Domain), list_to_atom(Operation)}
    end.

read_file(Path) ->
    case file:read_file(Path) of
        {ok, SQL} -> SQL;
        {error, Reason} -> erlang:error({read_error, Path, Reason})
    end.

reject_collisions(Entries) ->
    Counts = lists:foldl(
        fun({Key, _SQL}, Acc) -> maps:update_with(Key, fun(N) -> N + 1 end, 1, Acc) end,
        #{},
        Entries
    ),
    case [Key || Key := Count <- Counts, Count > 1] of
        [] ->
            ok;
        [Key | _] ->
            erlang:error({duplicate_sql_key, Key}, [], [{error_info, #{module => ?MODULE}}])
    end.

%% The roots this loader scans, Ergon's own `priv/queries` first. An explicit
%% `root` option replaces that default rather than extending it; `extra_roots`
%% appends to it.
roots(Opts) ->
    Default =
        case proplists:get_value(root, Opts) of
            undefined -> [filename:join(priv_dir(ergon), "queries")];
            Path -> [Path]
        end,
    Extra = [
        ergon_path:resolve_root(R)
     || R <- lists:flatten(proplists:get_all_values(extra_roots, Opts))
    ],
    Default ++ Extra.

priv_dir(App) ->
    case code:priv_dir(App) of
        {error, bad_name} -> erlang:error({priv_dir_not_found, App});
        Dir -> Dir
    end.

%% ---------------
%% Error formatting (EEP-54)
%% ---------------

-doc false.
-spec format_error(term(), erlang:stacktrace()) -> #{pos_integer() | general => unicode:chardata()}.
format_error(unknown_sql_key, [{_M, _F, _Args, Info} | _]) ->
    #{key := {Domain, Operation} = Key} = cause(Info),
    #{
        1 => iolist_to_binary(
            io_lib:format(
                "no SQL registered for ~p. Known keys: ~p. "
                "Expected a file at priv/queries/~s/~s.sql",
                [Key, keys(), Domain, Operation]
            )
        )
    };
format_error({top_level_sql_file, Path}, _Stacktrace) ->
    #{
        general => iolist_to_binary(
            io_lib:format(
                "SQL file ~ts must live under <root>/<domain>/<operation>.sql; "
                "top-level files are not allowed",
                [Path]
            )
        )
    };
format_error({duplicate_sql_key, Key}, _Stacktrace) ->
    #{
        general => iolist_to_binary(
            io_lib:format("duplicate SQL key ~p across the configured query roots", [Key])
        )
    };
format_error(_Reason, _Stacktrace) ->
    #{}.

cause(Info) ->
    maps:get(cause, proplists:get_value(error_info, Info, #{}), #{}).
