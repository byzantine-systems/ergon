-module(ergon_worker_SUITE).
-moduledoc """
Jobs running end to end through a real worker.

**Committed, not sandboxed.** A worker polls and a runner executes from processes
of their own, and the fixture transaction is bound to the case process's
dictionary, so neither would see a row written inside it. There is no `pgo`
equivalent of Ecto's `Sandbox.allow`. Every case therefore writes committed rows
under a queue prefix of its own and deletes them afterwards.

This suite also absorbs the old `job_fsm_test`. The Elixir port had a process
wrapping the pure state machine; here that role belongs to `ergon_job_runner`,
so "drives a job through a successful lifecycle" and "a failed run retries while
attempts remain" are assertions about what ends up in the database rather than
about a `gen_statem`.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    a_job_runs_and_completes/1,
    a_failing_job_retries_until_attempts_are_spent/1,
    a_raising_handler_is_an_error_not_a_crash/1,
    concurrency_bounds_jobs_in_flight/1,
    a_blocked_child_runs_once_its_parent_completes/1,
    a_slow_handler_is_stopped_at_the_deadline/1,
    stop_worker_stops_the_queue_tree/1
]).

%% Long enough that a poll is not the thing under test, short enough that a case
%% finishes promptly.
-define(POLL, 50).
-define(WAIT, 5000).

all() ->
    [
        a_job_runs_and_completes,
        a_failing_job_retries_until_attempts_are_spent,
        a_raising_handler_is_an_error_not_a_crash,
        concurrency_bounds_jobs_in_flight,
        a_blocked_child_runs_once_its_parent_completes,
        a_slow_handler_is_stopped_at_the_deadline,
        stop_worker_stops_the_queue_tree
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    [{queue, ergon_test_db:unique(~"wrk")} | Config].

end_per_testcase(_Case, Config) ->
    Queue = ?config(queue, Config),
    [ergon:stop_worker(Pid) || Pid <- worker_pids(Config)],
    ergon_test_db:cleanup_jobs(Queue).

%% ---------------
%% Cases
%% ---------------

a_job_runs_and_completes(Config) ->
    Queue = ?config(queue, Config),
    Self = self(),
    {ok, #{id := Id}} = enqueue(Queue, #{~"n" => 1}),

    start_worker(Config, Queue, fun(Job) ->
        Self ! {ran, maps:get(id, Job), maps:get(payload, Job)},
        ok
    end),

    receive
        {ran, Id, Payload} -> ?assertEqual(#{~"n" => 1}, Payload)
    after ?WAIT -> ct:fail(handler_never_ran)
    end,

    ?assertEqual(completed, await_state(Id, completed)),
    %% One attempt consumed, and the error field cleared on success.
    ?assertMatch(#{attempt := 1, last_error := null}, job(Id)).

%% max_attempts 2, so: run, error, retry, error, failed. The retry is what proves
%% the outcome is persisted and the job re-enters the fetch index.
a_failing_job_retries_until_attempts_are_spent(Config) ->
    Queue = ?config(queue, Config),
    Self = self(),
    Spec = ergon_new_job:with_max_attempts(
        ergon_new_job:on_queue(ergon_new_job:new(~"w"), Queue), 2
    ),
    {ok, #{id := Id}} = ergon:enqueue(Spec),

    start_worker(Config, Queue, fun(_Job) ->
        Self ! attempted,
        {error, ~"boom"}
    end),

    ?assertEqual(ok, await_message(attempted)),
    ?assertEqual(ok, await_message(attempted)),

    ?assertEqual(failed, await_state(Id, failed)),
    ?assertMatch(#{attempt := 2, last_error := ~"boom"}, job(Id)).

%% One bad job must never take a worker down, so a raise is recorded as an error
%% like any other and the worker keeps draining.
a_raising_handler_is_an_error_not_a_crash(Config) ->
    Queue = ?config(queue, Config),
    Self = self(),
    {ok, #{id := Boom}} = enqueue(Queue, #{~"boom" => true}),

    start_worker(Config, Queue, fun(Job) ->
        case maps:get(payload, Job) of
            #{~"boom" := true} ->
                error(deliberate);
            _ ->
                Self ! ran_the_next_one,
                ok
        end
    end),

    %% Waiting on `available` would be vacuous: it starts available and comes
    %% back to it. The error being recorded is the observable event.
    Failed = await_job(Boom, fun(#{last_error := E}) -> E =/= null end),
    ?assertMatch(#{state := available, attempt := 1}, Failed),

    %% The worker is still alive and still draining.
    {ok, _} = enqueue(Queue, #{~"boom" => false}),
    ?assertEqual(ok, await_message(ran_the_next_one)).

%% A checked-out job is already `executing` with an attempt consumed, so
%% over-fetching parks live jobs in a mailbox where a restart strands them. The
%% poller sizes each checkout against free capacity for exactly this reason.
concurrency_bounds_jobs_in_flight(Config) ->
    Queue = ?config(queue, Config),
    Self = self(),
    Release = make_ref(),
    [{ok, _} = enqueue(Queue, #{~"n" => N}) || N <- lists:seq(1, 6)],

    start_worker(
        Config,
        ergon_queue:with_concurrency(base_queue(Queue), 2),
        fun(_Job) ->
            Self ! {started, self()},
            receive
                Release -> ok
            after ?WAIT -> ok
            end
        end
    ),

    %% Two handlers start; a third must not, because the pool is full.
    Pids = [await_started(), await_started()],
    ?assertEqual(2, length(lists:usort(Pids))),
    ?assertEqual(timeout, await_started(300)),

    %% And no more than two jobs are executing in the database either.
    ?assert(state_count(Queue, ~"executing") =< 2),

    [P ! Release || P <- Pids].

%% The workflow guard, exercised through a live worker rather than through
%% checkout directly: the child must not run until its parent completes, and then
%% must run without anyone re-enqueueing it.
a_blocked_child_runs_once_its_parent_completes(Config) ->
    Prefix = ?config(queue, Config),
    ParentQueue = <<Prefix/binary, "_parent">>,
    ChildQueue = <<Prefix/binary, "_child">>,
    Self = self(),

    {ok, #{id := Parent}} = enqueue(ParentQueue, #{}),
    {ok, #{id := Child}} = enqueue(ChildQueue, #{}),
    ok = ergon:depends_on(Parent, Child),

    start_worker(Config, ChildQueue, fun(_Job) ->
        Self ! child_ran,
        ok
    end),

    %% Nothing runs while the parent is outstanding.
    ?assertEqual(timeout, await_message(child_ran, 500)),

    start_worker(Config, ParentQueue, fun(_Job) ->
        Self ! parent_ran,
        ok
    end),
    ?assertEqual(ok, await_message(parent_ran)),
    ?assertEqual(ok, await_message(child_ran)),
    ?assertEqual(completed, await_state(Child, completed)).

%% Past the deadline the handler is abandoned rather than waited on. The job goes
%% back to available with the timeout recorded, so it is retried rather than
%% silently lost.
a_slow_handler_is_stopped_at_the_deadline(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := Id}} = enqueue(Queue, #{}),

    start_worker(
        Config,
        ergon_queue:with_handler_timeout(base_queue(Queue), 200),
        fun(_Job) ->
            timer:sleep(?WAIT * 2),
            ok
        end
    ),

    Timed = await_job(Id, fun(#{last_error := E}) -> E =/= null end),
    ?assertMatch(#{state := available, attempt := 1}, Timed).

stop_worker_stops_the_queue_tree(Config) ->
    Queue = ?config(queue, Config),
    Self = self(),
    Pid = start_worker(Config, Queue, fun(_Job) ->
        Self ! ran,
        ok
    end),

    {ok, _} = enqueue(Queue, #{}),
    ?assertEqual(ok, await_message(ran)),

    ?assertEqual(ok, ergon:stop_worker(Pid)),
    ?assertNot(is_process_alive(Pid)),

    %% Nothing drains the queue any more.
    {ok, _} = enqueue(Queue, #{}),
    ?assertEqual(timeout, await_message(ran, 500)),

    %% Stopping again is `ok`, not `{error, not_found}`: under
    %% `simple_one_for_one`, `supervisor:terminate_child/2` accepts a pid it has
    %% already terminated. `not_found` is reserved for a pid that was never a
    %% child of this supervisor at all.
    ?assertEqual(ok, ergon:stop_worker(Pid)),
    ?assertEqual({error, not_found}, ergon:stop_worker(self())).

%% ---------------
%% Helpers
%% ---------------

base_queue(Name) ->
    ergon_queue:with_poll_interval(ergon_queue:new(Name), ?POLL).

start_worker(Config, Name, Handler) when is_binary(Name) ->
    start_worker(Config, base_queue(Name), Handler);
start_worker(Config, Queue, Handler) ->
    {ok, Pid} = ergon:start_worker(Queue, Handler),
    Started = ?config(workers, Config),
    put(workers, [Pid | started(Started)]),
    Pid.

started(undefined) -> [];
started(L) -> L.

%% Read back out of the process dictionary rather than threaded through Config,
%% because a case starts workers after init_per_testcase has already returned.
worker_pids(_Config) ->
    case erase(workers) of
        undefined -> [];
        Pids -> Pids
    end.

enqueue(Queue, Payload) ->
    ergon:enqueue(ergon_new_job:on_queue(ergon_new_job:new(~"w", Payload), Queue)).

job(Id) ->
    {ok, #{rows := [Row]}} = ergon_repo:query(
        [
            "SELECT ",
            ergon_job:column_list(),
            " FROM ergon.jobs_current WHERE id = $1"
        ],
        [Id]
    ),
    ergon_job:from_row(Row).

state_count(Queue, State) ->
    {ok, #{rows := [{N}]}} = ergon_repo:query(
        "SELECT count(*)::int FROM ergon.jobs_current WHERE queue = $1 AND state = $2::ergon.job_state",
        [Queue, State]
    ),
    N.

%% Poll rather than sleep a guessed interval: the worker writes the outcome from
%% its own process, so there is no message to wait on.
%%
%% Take care that the predicate is not already true when the wait begins, or the
%% case passes without the worker having done anything. Waiting for `available`
%% on a job that starts available is the trap.
await_state(Id, Expected) ->
    case await_job(Id, fun(#{state := S}) -> S =:= Expected end) of
        {timeout, #{state := Actual}} -> Actual;
        #{state := Actual} -> Actual
    end.

await_job(Id, Pred) -> await_job(Id, Pred, ?WAIT).

await_job(Id, _Pred, Remaining) when Remaining =< 0 ->
    {timeout, job(Id)};
await_job(Id, Pred, Remaining) ->
    Job = job(Id),
    case Pred(Job) of
        true ->
            Job;
        false ->
            timer:sleep(25),
            await_job(Id, Pred, Remaining - 25)
    end.

await_message(Message) -> await_message(Message, ?WAIT).

await_message(Message, Timeout) ->
    receive
        Message -> ok
    after Timeout -> timeout
    end.

await_started() -> await_started(?WAIT).

await_started(Timeout) ->
    receive
        {started, Pid} -> Pid
    after Timeout -> timeout
    end.
