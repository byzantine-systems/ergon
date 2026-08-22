-module(ergon_health_SUITE).
-moduledoc """
The liveness and diagnostics probe.

`blocked_is_reported_separately_from_runnable/1` is the case this suite exists
for. Phase 4 made a `failed` or `discarded` parent block its children
indefinitely, which is the honest reading of a dependency but leaves a queue that
looks idle while holding work that will never run. `runnable` cannot show it,
`ready_children/0` answers the opposite question, and nothing else surfaces it at
all.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    check_reports_every_section/1,
    db_is_ok_when_the_pool_is_reachable/1,
    extensions_are_name_to_version/1,
    pg_cron_is_absent_from_the_test_database/1,
    pgmq_defaults_to_the_configured_list/1,
    pgmq_queues_are_reported_per_queue/1,
    a_dropped_pgmq_queue_does_not_hide_the_others/1,
    blocked_is_reported_separately_from_runnable/1,
    counts_are_per_queue/1
]).

-define(SPEC(Queue), ergon_new_job:on_queue(ergon_new_job:new(~"w"), Queue)).

all() ->
    [
        check_reports_every_section,
        db_is_ok_when_the_pool_is_reachable,
        extensions_are_name_to_version,
        pg_cron_is_absent_from_the_test_database,
        pgmq_defaults_to_the_configured_list,
        pgmq_queues_are_reported_per_queue,
        a_dropped_pgmq_queue_does_not_hide_the_others,
        blocked_is_reported_separately_from_runnable,
        counts_are_per_queue
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    ok = ergon_test_db:sandbox(),
    [{queue, ergon_test_db:unique(~"hq")} | Config].

end_per_testcase(_Case, _Config) ->
    ok = ergon_test_db:rollback().

%% ---------------
%% Shape
%% ---------------

check_reports_every_section(_Config) ->
    ?assertMatch(
        #{db := _, extensions := _, jobs := _, pgmq := _},
        ergon_health:check()
    ).

db_is_ok_when_the_pool_is_reachable(_Config) ->
    ?assertEqual(ok, maps:get(db, ergon_health:check())).

extensions_are_name_to_version(_Config) ->
    Extensions = maps:get(extensions, ergon_health:check()),
    %% Installed unconditionally by priv/migrations/bootstrap.
    ?assert(maps:is_key(~"btree_gist", Extensions)),
    ?assert(maps:is_key(~"pgmq", Extensions)),
    ?assert(maps:is_key(~"pgcrypto", Extensions)),
    ?assert(byte_size(maps:get(~"pgmq", Extensions)) > 0).

%% Not an incidental property of the environment: `cron.database_name` is
%% cluster-wide and points at the dev database, so the guard in
%% priv/migrations/bootstrap skips pg_cron here. This is the assertion that the
%% same migration set really does run in both places.
pg_cron_is_absent_from_the_test_database(_Config) ->
    Extensions = maps:get(extensions, ergon_health:check()),
    ?assertNot(maps:is_key(~"pg_cron", Extensions)).

%% ---------------
%% pgmq
%% ---------------

%% pgmq has no notion of which queues belong to which application, so reporting
%% on every queue in the database would include other applications'. The list is
%% configuration, and empty by default.
pgmq_defaults_to_the_configured_list(_Config) ->
    ?assertEqual(#{}, maps:get(pgmq, ergon_health:check())).

pgmq_queues_are_reported_per_queue(_Config) ->
    Queue = ergon_test_db:unique(~"health_pgmq"),
    ok = ergon_pgmq:create_queue(Queue),

    Pgmq = maps:get(pgmq, ergon_health:check(#{pgmq_queues => [Queue]})),
    ?assertMatch(#{queue_length := 0}, maps:get(Queue, Pgmq)).

%% Reported per queue rather than aborting the whole check, so one dropped queue
%% does not hide the state of the others.
a_dropped_pgmq_queue_does_not_hide_the_others(_Config) ->
    Live = ergon_test_db:unique(~"health_live"),
    Missing = ergon_test_db:unique(~"health_missing"),
    ok = ergon_pgmq:create_queue(Live),

    Pgmq = maps:get(pgmq, ergon_health:check(#{pgmq_queues => [Live, Missing]})),
    ?assertMatch(#{queue_length := 0}, maps:get(Live, Pgmq)),
    ?assertMatch({error, _}, maps:get(Missing, Pgmq)).

%% ---------------
%% Jobs
%% ---------------

%% A blocked child is available but invisible to checkout, so it must not be
%% counted as runnable. Then completing the parent has to move it, which is what
%% says the two counts really are reading the same predicate from opposite sides.
blocked_is_reported_separately_from_runnable(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := Parent}} = ergon:enqueue(?SPEC(Queue)),
    {ok, _Child} = ergon:enqueue(?SPEC(Queue)),
    [_, Child] = queue_job_ids(Queue),
    ok = ergon:depends_on(Parent, Child),

    ?assertMatch(#{runnable := 1, blocked := 1}, metrics(Queue)),

    {ok, [Job]} = ergon_db:checkout(Queue, 1),
    ?assertEqual(Parent, maps:get(id, Job)),
    ?assertMatch(#{runnable := 0, blocked := 1, executing := 1}, metrics(Queue)),

    {ok, Outcome} = ergon_fsm:transition(Job, succeeded),
    {ok, _} = ergon_db:apply_outcome(Parent, Outcome),
    ?assertMatch(#{runnable := 1, blocked := 0, executing := 0}, metrics(Queue)).

%% Job queues need no configured list, unlike pgmq: they are rows in ergon.jobs,
%% so the query finds them.
counts_are_per_queue(_Config) ->
    A = ergon_test_db:unique(~"hq_a"),
    B = ergon_test_db:unique(~"hq_b"),
    {ok, _} = ergon:enqueue(?SPEC(A)),
    {ok, _} = ergon:enqueue(?SPEC(B)),
    {ok, _} = ergon:enqueue(?SPEC(B)),

    ?assertMatch(#{runnable := 1}, metrics(A)),
    ?assertMatch(#{runnable := 2}, metrics(B)).

%% ---------------
%% Helpers
%% ---------------

metrics(Queue) ->
    maps:get(Queue, maps:get(jobs, ergon_health:check())).

queue_job_ids(Queue) ->
    #{rows := Rows} = ergon_test_db:query(
        "SELECT id FROM ergon.jobs_current WHERE queue = $1 ORDER BY id", [Queue]
    ),
    [Id || {Id} <- Rows].
