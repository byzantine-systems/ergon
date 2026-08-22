-module(ergon_worker).
-moduledoc """
The polling loop for one queue.

On each tick a worker checks out a batch of jobs and casts them to its queue's
executor pool, then schedules the next tick. It does not run handlers itself;
`ergon_job_runner` does, so a slow handler never delays the next poll.

The periodic poll is the reliable path. On init the worker also joins
`ergon_worker_registry` under its queue name, so `ergon_job_notifier` can send it
a `wake` within a tick of a job becoming runnable and it drains immediately
rather than waiting out `poll_interval`. With the notifier disabled, pg_cron
absent, or a wake lost, the poll still drains everything, only later.

## Backpressure

A checked-out job is already `executing` in the database with an attempt
consumed. Fetching more than the pool can run therefore does not just queue work,
it parks live jobs in a mailbox where a node restart strands them until the
reconciler notices. So each poll fetches at most the pool's free capacity, read
from a shared `counters` reference, and fetches nothing at all when the pool is
saturated.

That gate is why dispatch can be a `cast`. `wpool:call/4` would supply
backpressure of its own, but it is synchronous per request whatever the strategy,
which would serialise the batch and block this process against `wake` while a
handler runs.

## The tick is scheduled on an absolute deadline

Draining takes time, so scheduling the next tick `poll_interval` from *now*
yields a real period of `interval + drain_time` that drifts under load. The next
deadline is instead advanced by exactly one interval and scheduled with
`{abs, true}`. When a drain overruns its interval the deadlines collapse to
"immediately" rather than accumulating a backlog of timers.
""".

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2, handle_info/2]).

-include_lib("kernel/include/logger.hrl").
-include_lib("ergon/include/ergon.hrl").

-define(IN_FLIGHT_IX, 1).

-type args() :: #{
    queue := queue(),
    pool := atom(),
    in_flight := counters:counters_ref()
}.
-export_type([args/0]).

-spec start_link(args()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec init(args()) -> {ok, map(), {continue, poll}}.
init(#{queue := #{name := Name, poll_interval := Interval}} = Args) ->
    proc_lib:set_label({?MODULE, Name}),
    %% Register for wake-ups before the first poll. Harmless when the notifier is
    %% disabled; the registry is always up, started ahead of the worker tree.
    ok = ergon_worker_registry:join(Name),
    State = Args#{deadline => initial_deadline(Interval)},
    %% Drain as soon as the worker is running rather than waiting out a tick.
    {ok, State, {continue, poll}}.

handle_continue(poll, State) ->
    {noreply, poll(State)}.

handle_info(poll, State) ->
    {noreply, poll(State)};
%% Fast-path wake from ergon_job_notifier: a job just landed on this queue, so
%% drain now. No timer is scheduled here, the poll loop keeps running underneath
%% as the fallback, untouched.
handle_info(wake, State) ->
    ok = drain(State),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

%% ---------------
%% Polling
%% ---------------

poll(State) ->
    ok = drain(State),
    schedule_next(State).

%% Phase, not period. A queue's `concurrency` workers, and every queue on the
%% node, otherwise seed their deadlines within a millisecond of each other at
%% boot and then poll in lockstep for the life of the node, so the checkout load
%% arrives as one spike per interval rather than spread across it. Backdating the
%% seed by a uniform draw within one interval puts the first tick somewhere
%% inside that interval and disperses them permanently, at no cost to the period:
%% schedule_next/1 still advances by whole intervals from here.
%%
%% `rand` is left unseeded on purpose. OTP gives each process its own state from
%% a unique source on first use, which is exactly what makes two workers started
%% in the same millisecond draw differently.
initial_deadline(Interval) ->
    erlang:monotonic_time(millisecond) - Interval + rand:uniform(Interval).

%% Advance the deadline by whole intervals until it is in the future, then sleep
%% until it. `max/2` covers the case where a drain took longer than an interval:
%% the next tick fires immediately instead of the worker trying to catch up on
%% ticks it has already missed.
schedule_next(#{queue := #{poll_interval := Interval}, deadline := Deadline} = State) ->
    Now = erlang:monotonic_time(millisecond),
    Next = max(Deadline + Interval, Now),
    _ = erlang:send_after(Next, self(), poll, [{abs, true}]),
    State#{deadline := Next}.

drain(#{queue := Queue} = State) ->
    #{name := Name, batch_size := BatchSize} = Queue,
    case budget(State, BatchSize) of
        0 ->
            %% Pool saturated. Skip the round-trip entirely rather than checking
            %% out jobs that would sit in a mailbox.
            ok;
        Limit ->
            case ergon_db:checkout(Name, Limit) of
                {ok, Jobs} ->
                    dispatch(State, Jobs);
                {error, Reason} ->
                    %% A transient checkout failure is skipped; the next tick
                    %% retries. Nothing has been claimed, so nothing is stranded.
                    ?LOG_WARNING(#{at => checkout_failed, queue => Name, reason => Reason}),
                    ok
            end
    end.

budget(#{queue := #{concurrency := Concurrency}, in_flight := Counter}, BatchSize) ->
    max(0, min(BatchSize, Concurrency - counters:get(Counter, ?IN_FLIGHT_IX))).

%% Claim the slot before casting, never after: the runner decrements on
%% completion, and a runner that finished before this process got round to
%% incrementing would drive the counter negative and inflate the budget.
dispatch(#{pool := Pool, in_flight := Counter}, Jobs) ->
    _ = [
        begin
            counters:add(Counter, ?IN_FLIGHT_IX, 1),
            wpool:cast(Pool, {run, Job})
        end
     || Job <- Jobs
    ],
    ok.
