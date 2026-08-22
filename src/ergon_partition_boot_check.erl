-module(ergon_partition_boot_check).
-moduledoc """
Boot-time fail-safe for a monthly-partitioned table.

The weekly `partition-lifecycle-<table>` cron job is what normally keeps the
partition horizon ahead of ingestion. This closes the gap when it has not: a
silently dead cron daemon, a clock-skewed failover, or a fresh restore of an old
backup all leave ingestion facing a table that cannot accept inserts. On every
boot this verifies the next `months_ahead` months exist and, if any are missing,
calls `auto_manage_partitions_<table>` directly.

Start it under the host's supervisor, ahead of anything that writes to the table:

```erlang
#{id => partitions,
  start => {ergon_partition_boot_check, start_link,
            [#{table => ~"telemetry", months_ahead => 2}]}}
```

## It blocks the supervisor on purpose

The check runs in `init/1` rather than in a `handle_continue`, so the host's
supervisor start-up waits for it. That is deliberate and is the whole design:
children later in the tree, the ingest pipeline and the endpoint, must not come
up against a table that cannot accept inserts. Failing to boot is a better
outcome than booting into guaranteed write errors.

For the same reason, if partitions are **still** missing after remediation it
raises rather than logging. At that point something is wrong that a retry will
not fix, most likely the manage function not existing at all, and starting
anyway would only move the failure somewhere less obvious.

Once the check passes the process has nothing left to do, so it hibernates rather
than sitting on its heap for the life of the node.
""".

-behaviour(gen_server).

-export([start_link/1, ensure_partitions/1, ensure_partitions/2, missing_partitions/1]).
-export([init/1, handle_call/3, handle_cast/2]).

-include_lib("kernel/include/logger.hrl").
-include_lib("ergon/include/ergon.hrl").

-define(DEFAULT_MONTHS_AHEAD, 2).

-type opts() :: #{
    table := binary(),
    months_ahead => non_neg_integer(),
    manage_fn => binary(),
    enabled => boolean()
}.
-export_type([opts/0]).

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec init(opts()) -> {ok, map(), hibernate}.
init(#{table := Table} = Opts) ->
    Name = ergon_ident:validate(Table, table),
    proc_lib:set_label({?MODULE, Name}),
    State = normalise(Opts),
    case maps:get(enabled, Opts, enabled()) of
        true -> ok = ensure_partitions(State);
        false -> ok
    end,
    {ok, State, hibernate}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

%% ---------------
%% The check
%% ---------------

-doc "Verify the horizon for `Table`, remediating if needed.".
-spec ensure_partitions(binary() | opts()) -> ok.
ensure_partitions(Table) when is_binary(Table) ->
    ensure_partitions(Table, ?DEFAULT_MONTHS_AHEAD);
ensure_partitions(Opts) when is_map(Opts) ->
    check(normalise(Opts)).

-doc """
Verify that partitions exist from the current month through `MonthsAhead`,
creating any that are missing.

Raises if they are still missing afterwards. See the module docs for why that is
the right response rather than a warning.
""".
-spec ensure_partitions(binary(), non_neg_integer()) -> ok.
ensure_partitions(Table, MonthsAhead) ->
    ensure_partitions(#{table => Table, months_ahead => MonthsAhead}).

check(#{table := Name, months_ahead := MonthsAhead, manage_fn := ManageFn} = State) ->
    case missing(Name, MonthsAhead) of
        [] ->
            ok;
        Missing ->
            ?LOG_WARNING(#{
                at => partitions_missing,
                table => Name,
                missing => Missing,
                remediating_with => ManageFn
            }),
            ok = remediate(ManageFn, MonthsAhead),
            case missing(Name, MonthsAhead) of
                [] ->
                    ?LOG_NOTICE(#{at => partitions_remediated, table => Name, created => Missing}),
                    ok;
                StillMissing ->
                    erlang:error(
                        {partitions_still_missing, Name, StillMissing},
                        [State],
                        [{error_info, #{module => ?MODULE}}]
                    )
            end
    end.

-doc "The `YYYYMM` labels in the horizon that lack a partition. Empty when healthy.".
-spec missing_partitions(binary()) -> [binary()].
missing_partitions(Table) ->
    missing(ergon_ident:validate(Table, table), ?DEFAULT_MONTHS_AHEAD).

missing(Name, MonthsAhead) ->
    case ergon_sql:query({partitions, missing}, [MonthsAhead, Name]) of
        {ok, #{rows := Rows}} -> [Month || {Month} <:- Rows];
        {error, Reason} -> erlang:error({partition_check_failed, Name, Reason})
    end.

%% The function name is interpolated because a function cannot be addressed by
%% bind parameter. It has been through ergon_ident:validate/2, either as the
%% default derived from the table name or as a caller-supplied override.
remediate(ManageFn, MonthsAhead) ->
    case ergon_repo:query(["SELECT ", ManageFn, "($1)"], [MonthsAhead]) of
        {ok, _} -> ok;
        {error, Reason} -> erlang:error({partition_remediation_failed, ManageFn, Reason})
    end.

%% ---------------
%% Configuration
%% ---------------

normalise(#{table := Table} = Opts) ->
    Name = ergon_ident:validate(Table, table),
    ManageFn =
        case maps:find(manage_fn, Opts) of
            {ok, Fn} -> ergon_ident:validate(Fn, function);
            error -> <<"auto_manage_partitions_", Name/binary>>
        end,
    #{
        table => Name,
        months_ahead => maps:get(months_ahead, Opts, ?DEFAULT_MONTHS_AHEAD),
        manage_fn => ManageFn
    }.

%% Defaults on, disabled through application environment. Tests turn it off
%% globally and start supervised instances with `enabled => true`, because the
%% application boots before any fixture table exists.
enabled() ->
    proplists:get_value(enabled, application:get_env(ergon, ?MODULE, []), true).
