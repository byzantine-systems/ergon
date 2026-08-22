-module(ergon_pgmq_consumer).
-moduledoc """
Streams messages off a pgmq queue, one batch at a time.

Replaces the Broadway pipeline. What it keeps is the delivery contract; what it
drops is the demand protocol, which existed to move backpressure between stages
that no longer exist.

## The cycle

Read a batch, hand every message to the executor pool, wait for all of them, then
archive the ones that succeeded in a single call. Repeat.

That shape is the backpressure, which is why there is no equivalent of
`ergon_worker`'s in-flight counter here: exactly one batch is ever outstanding,
and nothing is read until the last one is accounted for. Do not add a counter by
analogy with the job side. It would be measuring something that cannot exceed one
batch.

A full batch means there is probably more waiting, so the next cycle starts
immediately rather than sleeping out the poll interval. A short batch means the
queue is drained, and the consumer goes back to waiting.

## Archive means acknowledge

A message is archived only after its handler returned `ok`. A failure is reported
by *not* archiving: the visibility timeout expires and pgmq redelivers. There is
no negative acknowledgement, and nothing here deletes a message outright.

That is what makes delivery survive a BEAM crash mid-batch. Whatever was in
flight was never archived, so it comes back. The cost is that redelivery is
at-least-once, so handlers must tolerate seeing a message twice.

Two consequences of batching worth knowing:

- One slow message delays its siblings' archive, since the cycle waits for the
  whole batch. Bounded by `handler_timeout`, which defaults to the visibility
  timeout.
- A message whose handler always fails is redelivered forever. pgmq has no
  dead-letter queue; `read_ct` counts deliveries and is the only signal a handler
  has to give up on one.

## Waking up

The poll is always underneath. On top of it, optionally, either a `LISTEN`
subscription through the node's shared `ergon_listener`, or a server-side long
poll that blocks until a message arrives. See `ergon_pgmq_queue` for which to
pick.
""".

-behaviour(gen_server).

-export([start_link/1, child_spec/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2, handle_info/2]).

-include_lib("kernel/include/logger.hrl").
-include_lib("ergon/include/ergon.hrl").

%% Resubscribe backoff, in milliseconds. The floor is what the delay used to be
%% flat at, so a single restarted listener is picked up just as quickly; the
%% ceiling is a reconnect ceiling and has nothing to do with a message's.
-define(RESUBSCRIBE_MIN, 500).
-define(RESUBSCRIBE_MAX, 30_000).

-type args() :: #{
    queue := pgmq_queue(),
    pool := atom(),
    read_opts := query_options()
}.
-export_type([args/0]).

-spec start_link(args()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-doc """
Child spec for a host that supervises its own pipelines.

`ergon:start_consumer/2` is the usual way in, but a consumer is an ordinary
`gen_server` and a host that already owns a supervision tree may prefer to place
it there. Note this spec covers the consumer alone: started this way, it has no
executor pool of its own, so it must be given one.
""".
-spec child_spec(args()) -> supervisor:child_spec().
child_spec(Args) ->
    #{
        id => {?MODULE, maps:get(name, maps:get(queue, Args))},
        start => {?MODULE, start_link, [Args]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

-spec init(args()) -> {ok, map(), {continue, cycle}}.
init(#{queue := #{name := Name, poll_interval := Interval} = Queue} = Args) ->
    proc_lib:set_label({?MODULE, Name}),
    Backoff = backoff:type(backoff:init(?RESUBSCRIBE_MIN, ?RESUBSCRIBE_MAX), jitter),
    State = subscribe(Args#{
        deadline => initial_deadline(Interval),
        backoff => Backoff
    }),
    ?LOG_INFO(#{
        at => consumer_started,
        queue => Name,
        strategy => maps:get(read_strategy, Queue),
        notify => maps:get(notify_channel, Queue)
    }),
    {ok, State, {continue, cycle}}.

handle_continue(cycle, State) ->
    {noreply, cycle(State)}.

handle_info(poll, State) ->
    {noreply, cycle(State)};
%% A notification says only that work exists, never what it is. Run a cycle now
%% instead of waiting out the interval.
handle_info({notification, _Pid, _Ref, _Channel, _Payload}, State) ->
    {noreply, cycle(State)};
%% The shared listener died and took the subscription with it. Reconnects are
%% handled inside the driver, so this means the driver process itself crashed.
handle_info({'DOWN', MonRef, process, _Pid, Reason}, #{mon := MonRef} = State) ->
    ?LOG_WARNING(#{at => listener_down, reason => Reason}),
    {noreply, subscribe(State)};
handle_info(resubscribe, State) ->
    {noreply, subscribe(State)};
%% A reply from a batch that has already been settled: its deadline passed and
%% the cycle moved on. The message was reported failed and will be redelivered,
%% so there is nothing to do but drop this.
handle_info({pgmq_done, _StaleRef, _MsgId, _Result}, State) ->
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

%% ---------------
%% The cycle
%% ---------------

cycle(#{queue := Queue} = State) ->
    #{name := Name, visibility_timeout := Vt, batch_size := Size} = Queue,
    Strategy = maps:get(read_strategy, Queue),
    case ergon_pgmq:read(Name, Vt, Size, Strategy, maps:get(read_opts, State)) of
        {ok, []} ->
            schedule_next(State);
        {ok, Messages} ->
            ok = settle(State, Messages),
            case length(Messages) >= Size of
                %% A full batch: there is probably more behind it, so go again
                %% rather than sleeping. Long polling already blocks in the read,
                %% so this is the only thing keeping a backlog moving at speed.
                true -> cycle(State);
                false -> schedule_next(State)
            end;
        {error, Reason} ->
            %% Nothing was read, so nothing is held. The next cycle retries.
            ?LOG_WARNING(#{at => read_failed, queue => Name, reason => Reason}),
            schedule_next(State)
    end.

%% Dispatch the batch, wait for all of it, archive what succeeded.
settle(#{queue := #{name := Name} = Queue, pool := Pool} = State, Messages) ->
    BatchRef = make_ref(),
    _ = [wpool:cast(Pool, {run, self(), BatchRef, Message}) || Message <- Messages],
    Deadline = deadline(Queue),
    Succeeded = await(BatchRef, maps:from_keys([Id || #{id := Id} <- Messages], []), Deadline, []),
    case ergon_pgmq:archive(Name, Succeeded, maps:get(read_opts, State)) of
        {ok, _Archived} ->
            ok;
        {error, Reason} ->
            %% The handlers ran but the acknowledgement did not land, so every
            %% message in this batch will be redelivered. At-least-once holds;
            %% duplicated work does not violate the contract, but it is worth
            %% saying out loud because it is the one path that causes it.
            ?LOG_WARNING(#{
                at => archive_failed,
                queue => Name,
                count => length(Succeeded),
                reason => Reason
            }),
            ok
    end.

%% Collect one reply per dispatched message, or give up at the deadline.
%%
%% The deadline is a backstop, not the primary mechanism: each runner already
%% bounds its own handler, so this only fires if a runner itself is wedged. A
%% message not heard from is simply left unarchived, which is exactly how a
%% failure is reported anyway.
await(_BatchRef, Outstanding, _Deadline, Acc) when map_size(Outstanding) =:= 0 ->
    Acc;
await(BatchRef, Outstanding, Deadline, Acc) ->
    case remaining(Deadline) of
        0 ->
            Acc;
        Timeout ->
            receive
                {pgmq_done, BatchRef, MsgId, ok} ->
                    await(BatchRef, maps:remove(MsgId, Outstanding), Deadline, [MsgId | Acc]);
                {pgmq_done, BatchRef, MsgId, {error, _Reason}} ->
                    await(BatchRef, maps:remove(MsgId, Outstanding), Deadline, Acc)
            after Timeout ->
                Acc
            end
    end.

%% Half again the handler deadline: every runner should have finished or timed
%% out by then, so reaching this means a runner is stuck rather than a handler.
deadline(Queue) ->
    case ergon_pgmq_queue:effective_handler_timeout(Queue) of
        infinity -> infinity;
        Timeout -> erlang:monotonic_time(millisecond) + Timeout + (Timeout div 2)
    end.

remaining(infinity) -> infinity;
remaining(Deadline) -> max(0, Deadline - erlang:monotonic_time(millisecond)).

%% Phase, not period. Every consumer on a node otherwise seeds its deadline at
%% boot and polls in lockstep with all the others for the life of the node.
%% Backdating the seed by a uniform draw within one interval puts the first cycle
%% somewhere inside that interval and disperses them permanently, at no cost to
%% the period: schedule_next/1 still advances by whole intervals from here.
%%
%% `rand` is left unseeded on purpose. OTP gives each process its own state from
%% a unique source on first use, which is exactly what makes two consumers
%% started in the same millisecond draw differently.
initial_deadline(Interval) ->
    erlang:monotonic_time(millisecond) - Interval + rand:uniform(Interval).

%% Absolute scheduling, so the interval is the period rather than the gap
%% between cycles, and an overrunning cycle collapses to "immediately" instead of
%% queueing timers.
schedule_next(#{queue := #{poll_interval := Interval}, deadline := Deadline} = State) ->
    Now = erlang:monotonic_time(millisecond),
    Next = max(Deadline + Interval, Now),
    _ = erlang:send_after(Next, self(), poll, [{abs, true}]),
    State#{deadline := Next}.

%% ---------------
%% Notifications
%% ---------------

%% The retry is a jittered exponential rather than the flat 500 ms it used to be.
%% A consumer is per-queue, so what a database restart looks like from here is
%% every consumer on every node arriving at this clause within the same instant,
%% and a fixed delay holds them in that formation for as long as the outage
%% lasts. `backoff` in `jitter` mode spreads them and stops escalating at the
%% ceiling, drawing uniformly from `[max/3, max]` there rather than pinning every
%% consumer to one value. See `ergon.retry_backoff` for the article behind this.
subscribe(#{queue := #{notify_channel := undefined}} = State) ->
    State;
subscribe(#{queue := #{notify_channel := Channel}, backoff := Backoff} = State) ->
    case whereis(ergon_listener:name()) of
        undefined ->
            {Delay, Backoff1} = backoff:fail(Backoff),
            _ = erlang:send_after(Delay, self(), resubscribe),
            State#{backoff := Backoff1};
        _Pid ->
            {ok, SubRef, MonRef} = ergon_listener:subscribe(Channel),
            {_Delay, Backoff1} = backoff:succeed(Backoff),
            State#{sub => SubRef, mon => MonRef, backoff := Backoff1}
    end.
