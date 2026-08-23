-module(ergon_sql_SUITE).
-moduledoc """
The filesystem SQL loader, and the decoding that everything reading a job
depends on.

The case that earns its place here is `every_query_file_is_accounted_for/1`. It
asserts `keys/0` equals an explicit list, so a `.sql` file that is added without
a caller, renamed without its caller following, or lost in a move, fails here
rather than at the first call site that happens to run. That is exactly the class
of mistake a port makes.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    every_query_file_is_accounted_for/1,
    keys_are_domain_operation_pairs/1,
    find_returns_the_statement/1,
    fetch_raises_for_an_unknown_key/1,
    reload_recounts/1,
    parameterless_queries_execute/1,
    tstzrange_decodes_to_a_period/1,
    unbounded_upper_tstzrange_decodes/1
]).

%% Every statement under priv/queries, split by whether it can be executed with
%% no arguments. The parameterless ones are executed below; the rest name the
%% suite that exercises them, so the split is a map of coverage rather than an
%% inventory.
-define(PARAMETERLESS, [
    {graph, ready_children},
    {system, healthcheck},
    {system, installed_extensions},
    {system, job_metrics}
]).

-define(PARAMETERIZED, [
    %% ergon_db_SUITE
    {jobs, apply_outcome},
    {jobs, asof},
    {jobs, asof_system},
    {jobs, checkout},
    {jobs, discard_descendants},
    {jobs, insert},
    {jobs, link},
    %% ergon_graph_SUITE
    {graph, descendants},
    {graph, direct_children},
    {graph, lock_edges},
    {graph, would_create_cycle},
    %% ergon_reconciler_SUITE
    {jobs, pending_parents_drift},
    {jobs, repair_pending_parents},
    %% ergon_partition_boot_check_SUITE
    {partitions, missing},
    %% ergon_pgmq_SUITE
    {pgmq, archive},
    {pgmq, bind_topic},
    {pgmq, create_queue},
    {pgmq, disable_notify},
    {pgmq, drop_queue},
    {pgmq, enable_notify},
    {pgmq, list_topic_bindings},
    {pgmq, metrics},
    {pgmq, read},
    {pgmq, read_grouped},
    {pgmq, read_grouped_head},
    {pgmq, read_grouped_head_with_poll},
    {pgmq, read_grouped_rr},
    {pgmq, read_grouped_rr_with_poll},
    {pgmq, read_grouped_with_poll},
    {pgmq, read_with_poll},
    {pgmq, release_leases},
    {pgmq, send},
    {pgmq, send_topic},
    {pgmq, unbind_topic}
]).

all() ->
    [
        every_query_file_is_accounted_for,
        keys_are_domain_operation_pairs,
        find_returns_the_statement,
        fetch_raises_for_an_unknown_key,
        reload_recounts,
        parameterless_queries_execute,
        tstzrange_decodes_to_a_period,
        unbounded_upper_tstzrange_decodes
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    ok = ergon_test_db:sandbox(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = ergon_test_db:rollback().

%% ---------------
%% Loading
%% ---------------

every_query_file_is_accounted_for(_Config) ->
    Expected = lists:sort(?PARAMETERLESS ++ ?PARAMETERIZED),
    ?assertEqual(Expected, lists:sort(ergon_sql:keys())).

keys_are_domain_operation_pairs(_Config) ->
    Keys = ergon_sql:keys(),
    ?assert(lists:member({jobs, insert}, Keys)),
    ?assert(lists:member({graph, ready_children}, Keys)),
    ?assert(lists:member({pgmq, read}, Keys)),
    %% The directory becomes the domain and the basename the operation, so a
    %% file with an underscore in its name keeps it rather than splitting again.
    ?assert(lists:member({jobs, apply_outcome}, Keys)).

find_returns_the_statement(_Config) ->
    {ok, SQL} = ergon_sql:find({jobs, insert}),
    ?assertNotEqual(nomatch, binary:match(SQL, ~"ergon.enqueue")),
    %% find/1 and fetch/1 pair the way maps:find/2 and maps:get/2 do.
    ?assertEqual(error, ergon_sql:find({nope, missing})).

fetch_raises_for_an_unknown_key(_Config) ->
    ?assertError(unknown_sql_key, ergon_sql:fetch({nope, missing})).

reload_recounts(_Config) ->
    Before = length(ergon_sql:keys()),
    ?assertEqual(Before, ergon_sql:reload()),
    ?assertEqual(Before, length(ergon_sql:keys())).

%% Every statement that can run with no arguments, run. This is the half of the
%% coverage assertion that catches SQL which loads but does not parse.
parameterless_queries_execute(_Config) ->
    [
        ?assertMatch({ok, #{rows := _}}, ergon_sql:query(Key, []))
     || Key <- ?PARAMETERLESS
    ],
    ok.

%% ---------------
%% Decoding
%% ---------------

%% `valid_period` and `system_time` are tstzrange columns, and every job read
%% goes through this decoder. pg_range is generic over its base type, which is
%% why Ergon needs no hand-written binary codec: the whole conversion is
%% ergon_temporal_period:from_pg_range/1.
tstzrange_decodes_to_a_period(_Config) ->
    Range = ergon_test_db:scalar(
        "SELECT tstzrange('2020-01-01+00', '2030-01-01+00', '[)')"
    ),
    Period = ergon_temporal_period:from_pg_range(Range),
    ?assertMatch(
        #{
            lower := {{2020, 1, 1}, {0, 0, _}},
            upper := {{2030, 1, 1}, {0, 0, _}},
            lower_inclusive := true,
            upper_inclusive := false,
            empty := false
        },
        Period
    ).

%% The two ways a tstzrange can reach forever, which are not the same thing and
%% do not decode the same way.
%%
%% Ergon's schema writes `tstzrange(now(), 'infinity', '[)')`, a literal
%% timestamptz value. `upper_inf()` on it is **false** and the driver hands back
%% the atom `infinity`. A NULL upper bound is the genuinely unbounded one, and
%% that is what arrives as `unbound` and becomes `unbounded`.
%%
%% So no Ergon column ever produces `unbounded`: the whole schema keys liveness
%% off `upper(valid_period) = 'infinity'`, including the `is_live` generated
%% column and the `jobs_fetch_idx` predicate. The `unbounded` path exists for a
%% host's own tstzrange columns. Both are pinned here because the difference is
%% invisible until something compares the wrong one.
unbounded_upper_tstzrange_decodes(_Config) ->
    Literal = ergon_test_db:scalar("SELECT tstzrange(now(), 'infinity', '[)')"),
    ?assertMatch(
        #{lower := {{_, _, _}, {_, _, _}}, upper := infinity},
        ergon_temporal_period:from_pg_range(Literal)
    ),

    Null = ergon_test_db:scalar("SELECT tstzrange(now(), NULL)"),
    ?assertMatch(
        #{lower := {{_, _, _}, {_, _, _}}, upper := unbounded},
        ergon_temporal_period:from_pg_range(Null)
    ),

    %% And the reason it matters: a live job is `infinity`, not unbounded.
    ?assertEqual(false, ergon_test_db:scalar("SELECT upper_inf(tstzrange(now(), 'infinity'))")),
    ?assertEqual(true, ergon_test_db:scalar("SELECT upper_inf(tstzrange(now(), NULL))")).
