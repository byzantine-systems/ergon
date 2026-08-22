-module(ergon_reconciler_SUITE).
-moduledoc """
The recovery flow, and the derived-state check that has no other home.

Two things here are about ordering and defaults rather than about results, and
both are deliberate design decisions worth pinning:

- `hydrate` runs **before** leases are released. Releasing first would redeliver
  messages to processes the callback is about to stop.
- Drift is **reported** by default and repaired only on request. Silently
  rewriting derived state hides whatever caused it to drift, and the drift is the
  more useful signal.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    releases_stranded_leases_and_summarises/1,
    queue_stats_have_the_pgmq_shape/1,
    passes_through_multiple_queues/1,
    hydrate_result_is_captured/1,
    hydrate_runs_before_leases_are_released/1,
    hydrate_defaults_to_a_noop/1,
    drift_is_empty_when_healthy/1,
    drift_reports_the_disagreeing_row/1,
    drift_is_not_repaired_by_default/1,
    repair_rewrites_only_wrong_rows/1,
    run_repairs_only_when_asked/1
]).

-define(SPEC(Queue), ergon_new_job:on_queue(ergon_new_job:new(~"w"), Queue)).

all() -> [{group, pgmq}, {group, hydrate}, {group, drift}].

groups() ->
    [
        {pgmq, [], [
            releases_stranded_leases_and_summarises,
            queue_stats_have_the_pgmq_shape,
            passes_through_multiple_queues
        ]},
        {hydrate, [], [
            hydrate_result_is_captured,
            hydrate_runs_before_leases_are_released,
            hydrate_defaults_to_a_noop
        ]},
        {drift, [], [
            drift_is_empty_when_healthy,
            drift_reports_the_disagreeing_row,
            drift_is_not_repaired_by_default,
            repair_rewrites_only_wrong_rows,
            run_repairs_only_when_asked
        ]}
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    ok = ergon_test_db:sandbox(),
    Queue = ergon_test_db:unique(~"reconcile"),
    ok = ergon_pgmq:create_queue(Queue),
    [{queue, Queue} | Config].

end_per_testcase(_Case, _Config) ->
    ok = ergon_test_db:rollback().

%% ---------------
%% pgmq recovery
%% ---------------

releases_stranded_leases_and_summarises(Config) ->
    Q = ?config(queue, Config),
    {ok, Id} = ergon_pgmq:send(Q, #{~"x" => 1}),

    %% A consumer takes a long lease and dies without acking.
    {ok, [#{id := Id}]} = ergon_pgmq:read(Q, 3600, 10),
    ?assertEqual({ok, []}, ergon_pgmq:read(Q, 30, 10)),

    Summary = ergon_reconciler:run(#{pgmq_queues => [Q]}),
    Stats = maps:get(Q, maps:get(pgmq, Summary)),
    ?assertMatch(#{released_leases := 1, queue_length := 1}, Stats),

    %% The stranded message is deliverable again, with read_ct bumped.
    ?assertMatch({ok, [#{id := Id, read_ct := 2}]}, ergon_pgmq:read(Q, 30, 10)).

queue_stats_have_the_pgmq_shape(Config) ->
    Q = ?config(queue, Config),
    Summary = ergon_reconciler:run(#{pgmq_queues => [Q]}),
    ?assertMatch(
        #{
            released_leases := 0,
            queue_length := 0,
            queue_visible_length := 0,
            oldest_msg_age_sec := null
        },
        maps:get(Q, maps:get(pgmq, Summary))
    ).

passes_through_multiple_queues(Config) ->
    Q1 = ?config(queue, Config),
    Q2 = ergon_test_db:unique(~"reconcile"),
    ok = ergon_pgmq:create_queue(Q2),

    Summary = ergon_reconciler:run(#{pgmq_queues => [Q1, Q2]}),
    ?assertEqual(lists:sort([Q1, Q2]), lists:sort(maps:keys(maps:get(pgmq, Summary)))).

%% ---------------
%% hydrate
%% ---------------

hydrate_result_is_captured(Config) ->
    Q = ?config(queue, Config),
    Summary = ergon_reconciler:run(#{
        pgmq_queues => [Q],
        hydrate => fun() -> {stopped, 3} end
    }),
    ?assertEqual({stopped, 3}, maps:get(hydrate, Summary)).

%% The ordering assertion. A callback that reads the queue itself must still see
%% the lease held, because release_leases has not run yet.
hydrate_runs_before_leases_are_released(Config) ->
    Q = ?config(queue, Config),
    {ok, _} = ergon_pgmq:send(Q, #{~"x" => 2}),
    {ok, [_]} = ergon_pgmq:read(Q, 3600, 10),

    Summary = ergon_reconciler:run(#{
        pgmq_queues => [Q],
        hydrate => fun() ->
            {ok, Visible} = ergon_pgmq:read(Q, 30, 10),
            length(Visible)
        end
    }),

    ?assertEqual(0, maps:get(hydrate, Summary)),
    %% And afterwards it is visible again.
    ?assertMatch({ok, [_]}, ergon_pgmq:read(Q, 30, 10)).

hydrate_defaults_to_a_noop(Config) ->
    Summary = ergon_reconciler:run(#{pgmq_queues => [?config(queue, Config)]}),
    ?assertEqual(ok, maps:get(hydrate, Summary)).

%% ---------------
%% pending_parents drift
%% ---------------

drift_is_empty_when_healthy(_Config) ->
    {_Parent, _Child} = linked_pair(),
    ?assertEqual([], ergon_reconciler:drift()).

%% The counter is denormalised into the fetch index's predicate, so nothing on
%% the hot path reads job_edges any more and nothing else can notice it is wrong.
%% Too high and the job is permanently unrunnable; too low and it runs before its
%% parents finish.
drift_reports_the_disagreeing_row(_Config) ->
    {_Parent, Child} = linked_pair(),
    corrupt(Child, 5),

    ?assertEqual([#{id => Child, actual => 5, expected => 1}], ergon_reconciler:drift()).

drift_is_not_repaired_by_default(_Config) ->
    {_Parent, Child} = linked_pair(),
    corrupt(Child, 5),

    _ = ergon_reconciler:drift(),
    ?assertEqual(5, pending_parents(Child)).

%% Only the wrong rows are written, which matters because each write fires the
%% versioning trigger and accrues a history row.
repair_rewrites_only_wrong_rows(_Config) ->
    {_P1, Child1} = linked_pair(),
    {_P2, Child2} = linked_pair(),
    corrupt(Child1, 5),

    ?assertEqual([Child1], ergon_reconciler:repair_drift()),
    ?assertEqual(1, pending_parents(Child1)),
    ?assertEqual(1, pending_parents(Child2)),
    ?assertEqual([], ergon_reconciler:drift()).

run_repairs_only_when_asked(_Config) ->
    {_Parent, Child} = linked_pair(),
    corrupt(Child, 5),

    Reported = ergon_reconciler:run(#{}),
    ?assertEqual(not_attempted, maps:get(repaired, Reported)),
    ?assertMatch([#{id := Child}], maps:get(pending_parents_drift, Reported)),
    ?assertEqual(5, pending_parents(Child)),

    Repaired = ergon_reconciler:run(#{repair => true}),
    ?assertEqual([Child], maps:get(repaired, Repaired)),
    ?assertEqual(1, pending_parents(Child)).

%% ---------------
%% Helpers
%% ---------------

linked_pair() ->
    {ok, #{id := Parent}} = ergon:enqueue(?SPEC(ergon_test_db:unique(~"drift"))),
    {ok, #{id := Child}} = ergon:enqueue(?SPEC(ergon_test_db:unique(~"drift"))),
    ok = ergon:depends_on(Parent, Child),
    {Parent, Child}.

%% A direct write, which is one of the two ways the counter goes wrong in
%% practice; a bug in the maintaining triggers is the other. The CHECK is
%% deliberately unclamped so drift surfaces loudly rather than being absorbed.
corrupt(Id, Value) ->
    _ = ergon_test_db:query(
        "UPDATE ergon.jobs SET pending_parents = $2 "
        "WHERE id = $1 AND upper(valid_period) = 'infinity'",
        [Id, Value]
    ),
    ok.

pending_parents(Id) ->
    ergon_test_db:scalar(
        "SELECT pending_parents FROM ergon.jobs_current WHERE id = $1", [Id]
    ).
