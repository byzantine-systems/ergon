-module(ergon_partition_boot_check_SUITE).
-moduledoc """
The boot-time partition guard.

Committed, because the check runs its queries from its own process during
`init/1` and so cannot see a fixture transaction. Each case builds a partitioned
table of its own and drops it afterwards.

The design under test looks like a mistake and is not: the check runs in
`init/1` and **deliberately blocks the starting supervisor**, because children
later in the tree must not come up against a table that cannot accept inserts.
Failing to boot beats booting into guaranteed write errors, which is also why it
raises rather than logging when remediation does not work.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    horizon_is_complete_after_setup/1,
    a_dropped_future_partition_is_detected/1,
    ensure_partitions_recreates_them/1,
    ensure_partitions_is_a_noop_when_healthy/1,
    boot_remediates_before_returning/1,
    boot_can_be_disabled/1,
    boot_raises_without_a_manage_function/1,
    identifier_guard_rejects_bad_table_names/1
]).

all() ->
    [
        horizon_is_complete_after_setup,
        a_dropped_future_partition_is_detected,
        ensure_partitions_recreates_them,
        ensure_partitions_is_a_noop_when_healthy,
        boot_remediates_before_returning,
        boot_can_be_disabled,
        boot_raises_without_a_manage_function,
        identifier_guard_rejects_bad_table_names
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(identifier_guard_rejects_bad_table_names, Config) ->
    Config;
init_per_testcase(boot_raises_without_a_manage_function, Config) ->
    %% A partitioned parent with no manage function installed.
    Table = ergon_test_db:unique(~"pbc_bare"),
    ok = create_parent(Table),
    [{table, Table} | Config];
init_per_testcase(_Case, Config) ->
    Table = ergon_test_db:unique(~"pbc"),
    ok = create_parent(Table),
    ok = install_manager(Table),
    [{table, Table} | Config].

end_per_testcase(identifier_guard_rejects_bad_table_names, _Config) ->
    ok;
end_per_testcase(_Case, Config) ->
    Table = ?config(table, Config),
    stop_checks(),
    %% CASCADE takes the partitions with the parent.
    _ = ergon_repo:query(["DROP TABLE IF EXISTS ", Table, " CASCADE"], []),
    _ = ergon_repo:query(
        ["DROP FUNCTION IF EXISTS auto_manage_partitions_", Table, "(int)"], []
    ),
    ok.

%% ---------------
%% Detection
%% ---------------

horizon_is_complete_after_setup(Config) ->
    ?assertEqual([], ergon_partition_boot_check:missing_partitions(?config(table, Config))).

a_dropped_future_partition_is_detected(Config) ->
    Table = ?config(table, Config),
    Partition = partition_name(Table, 2),
    drop_partition(Partition),

    [Month] = ergon_partition_boot_check:missing_partitions(Table),
    ?assert(binary:longest_common_suffix([Partition, Month]) =:= byte_size(Month)).

%% ---------------
%% Remediation
%% ---------------

ensure_partitions_recreates_them(Config) ->
    Table = ?config(table, Config),
    drop_partition(partition_name(Table, 1)),
    drop_partition(partition_name(Table, 2)),
    ?assertEqual(2, length(ergon_partition_boot_check:missing_partitions(Table))),

    ?assertEqual(ok, ergon_partition_boot_check:ensure_partitions(Table)),
    ?assertEqual([], ergon_partition_boot_check:missing_partitions(Table)).

ensure_partitions_is_a_noop_when_healthy(Config) ->
    ?assertEqual(ok, ergon_partition_boot_check:ensure_partitions(?config(table, Config))).

%% ---------------
%% Boot
%% ---------------

%% init/1 blocks until remediation is complete, so if start_link/1 returns at
%% all, the partitions are already there. No waiting, no polling.
boot_remediates_before_returning(Config) ->
    Table = ?config(table, Config),
    drop_partition(partition_name(Table, 2)),
    ?assertEqual(1, length(ergon_partition_boot_check:missing_partitions(Table))),

    Pid = start_check(#{table => Table, enabled => true}),
    ?assert(is_pid(Pid)),
    ?assertEqual([], ergon_partition_boot_check:missing_partitions(Table)).

%% Disabled, init/1 must not touch the database at all. Hosts turn it off in
%% tests, where the application boots before any fixture table exists.
boot_can_be_disabled(Config) ->
    Table = ?config(table, Config),
    drop_partition(partition_name(Table, 2)),

    _Pid = start_check(#{table => Table, enabled => false}),
    %% Still missing: the check was correctly inert.
    ?assertEqual(1, length(ergon_partition_boot_check:missing_partitions(Table))).

%% Nothing a retry would fix, most likely the manage function never having been
%% installed, so it raises rather than logging and carrying on.
boot_raises_without_a_manage_function(Config) ->
    Table = ?config(table, Config),
    %% The parent exists but has no partitions and no way to make any.
    ?assert(length(ergon_partition_boot_check:missing_partitions(Table)) > 0),

    process_flag(trap_exit, true),
    Result = ergon_partition_boot_check:start_link(#{table => Table, enabled => true}),
    process_flag(trap_exit, false),
    ?assertMatch({error, _}, Result).

%% ---------------
%% Safety
%% ---------------

%% The manage function's name is interpolated, because a function cannot be
%% addressed by bind parameter. This is the third caller of the shared guard,
%% after ergon_pgmq and ergon_migration.
identifier_guard_rejects_bad_table_names(_Config) ->
    ?assertError(
        invalid_identifier,
        ergon_partition_boot_check:missing_partitions(~"evil; DROP TABLE x --")
    ),
    ?assertError(
        invalid_identifier, ergon_partition_boot_check:missing_partitions(~"1ev")
    ),
    ?assertError(
        invalid_identifier, ergon_partition_boot_check:ensure_partitions(~"Mixed")
    ).

%% ---------------
%% Helpers
%% ---------------

create_parent(Table) ->
    {ok, _} = ergon_repo:query(
        [
            "CREATE TABLE ",
            Table,
            " (id bigint NOT NULL, recorded_at timestamptz NOT NULL)"
            " PARTITION BY RANGE (recorded_at)"
        ],
        []
    ),
    ok.

install_manager(Table) ->
    [
        {ok, _} = ergon_repo:query(SQL, [])
     || SQL <- ergon_migration:partitioned_table_sql(Table, ~"recorded_at")
    ],
    ok.

%% The partition covering the month `MonthsOut` from now, named the way the
%% generated manage function names it.
partition_name(Table, MonthsOut) ->
    {ok, #{rows := [{Name}]}} = ergon_repo:query(
        "SELECT $1::text || '_' || to_char("
        "  date_trunc('month', now()) + make_interval(months => $2::int), 'YYYYMM')",
        [Table, MonthsOut]
    ),
    Name.

drop_partition(Partition) ->
    {ok, _} = ergon_repo:query(["DROP TABLE ", Partition], []),
    ok.

start_check(Opts) ->
    {ok, Pid} = ergon_partition_boot_check:start_link(Opts),
    put(checks, [Pid | existing()]),
    Pid.

existing() ->
    case get(checks) of
        undefined -> [];
        Pids -> Pids
    end.

stop_checks() ->
    [
        begin
            unlink(Pid),
            exit(Pid, shutdown)
        end
     || Pid <- lists:filter(fun is_process_alive/1, erase_checks())
    ],
    ok.

erase_checks() ->
    case erase(checks) of
        undefined -> [];
        Pids -> Pids
    end.
