-module(ergon_job_runner).
-moduledoc """
Executes one job: runs the host's handler, then persists the outcome.

`worker_pool` starts `concurrency` of these per queue and `ergon_worker` casts
each checked-out job to the pool. The pool is used purely as a supervised,
bounded set of executors. None of its worker-selection strategies apply here,
because `FOR UPDATE SKIP LOCKED` in `checkout` already decided which job goes to
which node and process. The database is the dispatcher.

## The handler runs in a child process

A handler is host code and may do anything: return garbage, raise, exit, or hang
forever. All four have to cost one job rather than an executor, so the handler
runs in a spawned child and this process waits on it with a deadline.

That the child is spawned with `spawn_request/2` rather than `spawn_monitor/1`
is deliberate. For a local spawn the two are otherwise equivalent, but this
process is a `gen_server` that `worker_pool` also manages, so its mailbox carries
traffic that is not ours. `{reply_tag, ergon_spawn}` and `{monitor, [{tag,
ergon_down}]}` keep Ergon's two possible replies unmistakable instead of
competing for the generic `'DOWN'` tag, and the `ReqId` doubles as the monitor
reference, so one term covers both "the spawn failed" and "the process died".
`{reply, error_only}` drops the success reply, leaving exactly one message on the
happy path.

## A timeout is an outcome, not an abandonment

On deadline the child is killed and the job is recorded as `{errored,
<<"handler timeout">>}`, which consumes an attempt and lets the database's
jittered backoff reschedule it. This is why `max_overrun_warnings` is left at
`infinity` in the pool configuration: wpool's own overrun enforcement kills the
*executor*, which would abandon the job mid-flight with no outcome written,
leaving the row `executing` until the reconciler reclaims it. The deadline here
kills the *handler* and still writes the outcome.

## The in-flight counter

The counter is decremented in an `after` clause, so a slot is returned even if
persisting the outcome fails. Leaking slots would starve the queue permanently:
`ergon_worker` sizes each checkout against free capacity, so a counter that only
ever rises eventually stops all fetching.
""".

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2]).

-include_lib("kernel/include/logger.hrl").
-include_lib("ergon/include/ergon.hrl").

-define(IN_FLIGHT_IX, 1).

-type args() :: #{
    queue := binary(),
    handler := handler(),
    handler_timeout := timeout(),
    in_flight := counters:counters_ref()
}.
-export_type([args/0]).

-spec start_link(args()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec init(args()) -> {ok, args()}.
init(#{queue := Queue} = Args) ->
    proc_lib:set_label({?MODULE, Queue}),
    {ok, Args}.

handle_cast({run, Job}, State) ->
    _ = run(State, Job),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

%% ---------------
%% Execution
%% ---------------

run(#{in_flight := Counter} = State, Job) ->
    try
        persist(Job, execute(State, Job))
    after
        counters:sub(Counter, ?IN_FLIGHT_IX, 1)
    end.

%% Run the handler in a child process and wait out `handler_timeout`.
execute(#{handler := Handler, handler_timeout := Timeout}, Job) ->
    ReqId = spawn_request(
        fun() -> exit({ergon_result, safe_run(Handler, Job)}) end,
        [
            {reply, error_only},
            {reply_tag, ergon_spawn},
            {monitor, [{tag, ergon_down}]}
        ]
    ),
    receive
        {ergon_down, ReqId, process, _Pid, {ergon_result, ok}} ->
            succeeded;
        {ergon_down, ReqId, process, _Pid, {ergon_result, {error, Reason}}} ->
            {errored, Reason};
        %% The child exited without producing a result: killed from outside, or
        %% brought down by a linked process. Not reachable through safe_run/2,
        %% which converts every raise and throw itself.
        {ergon_down, ReqId, process, _Pid, Abnormal} ->
            {errored, describe(Abnormal)};
        {ergon_spawn, ReqId, error, Reason} ->
            {errored, describe({spawn_failed, Reason})}
    after Timeout ->
        %% abandon first so a reply that is already in flight is discarded
        %% rather than left in the mailbox for the next job to trip over.
        _ = spawn_request_abandon(ReqId),
        {errored, ~"handler timeout"}
    end.

%% A handler that raises, throws, or returns something unexpected is an errored
%% job, never a crashed executor.
safe_run(Handler, Job) ->
    try Handler(Job) of
        ok -> ok;
        {error, Reason} when is_binary(Reason) -> {error, Reason};
        Other -> {error, describe({handler_returned, Other})}
    catch
        Class:Reason:Stacktrace ->
            {error, describe({Class, Reason, Stacktrace})}
    end.

%% Thread the event through the state machine, then write the result. A job that
%% cannot be finalised is logged and left for the next poll or an operator,
%% rather than taken as a reason to crash the executor.
-spec persist(job(), fsm_event()) -> ok.
persist(#{id := Id} = Job, Event) ->
    maybe
        {ok, Outcome} ?= ergon_fsm:transition(Job, Event),
        {ok, _Persisted} ?= ergon_db:apply_outcome(Id, Outcome),
        ok
    else
        Other ->
            ?LOG_WARNING(#{
                at => finalise_failed,
                job_id => Id,
                queue => maps:get(queue, Job),
                event => Event,
                reason => Other
            }),
            ok
    end.

describe(Term) ->
    iolist_to_binary(io_lib:format("~0p", [Term])).
