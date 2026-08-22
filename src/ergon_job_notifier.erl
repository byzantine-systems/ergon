-module(ergon_job_notifier).
-moduledoc """
Turns `ergon.jobs` into a reactive queue: one `LISTEN` connection that wakes the
right workers the instant a runnable job lands, instead of making them wait out
their poll interval.

One instance runs per node, subscribing to the fixed channel
`ergon_job_available` on the node's shared `ergon_listener` connection. The
emitting half is the pg_cron tick installed by
`priv/migrations/cron`: `ergon.notify_pending_jobs()` runs every second and fires
`pg_notify('ergon_job_available', queue)` once per queue holding immediately
runnable work. Each notification's payload is the **queue name only**, never job
data or tenant, since `NOTIFY` bypasses row-level security, and the notifier
hands it to `ergon_worker_registry:wake/1`.

## Writers never call pg_notify

This is the architectural point, and it is easy to undo by accident. A
transaction with a pending `NOTIFY` takes the global notification-queue lock at
commit, which serialises *every* notifying commit. A trigger firing `pg_notify`
per insert therefore caps enqueue throughput no matter how much hardware is
available. The 1 s cron tick is the batching mechanism that avoids it: at most
one notification per queue per second regardless of how many jobs arrive, so a
batch enqueue of a thousand jobs produces a single wake.

## The poll is still the durable path

`ergon.jobs` is the durable fact and `checkout`'s `FOR UPDATE SKIP LOCKED` is the
reliable puller, so a notification is a hint that cannot be depended on. Workers
keep their periodic fallback poll, which alone drains everything correctly. That
fallback covers the gap before the listener connects, every reconnect window, and
every deployment without pg_cron, where the tick never runs and no notification
ever fires. A dropped notification costs latency, never a stuck job.

Because the tick is level-triggered on "runnable work exists" rather than edge-
triggered on an insert, it keeps re-waking workers until a queue is drained, and
a future-scheduled retry wakes its workers on the first tick after `scheduled_at`
passes, precisely when it is due.

## Configuration

Optional. Disabled, workers simply poll: still fully correct, only slower.

```erlang
{ergon, [{ergon_job_notifier, [{enabled, false}]}]}
```

## A note on connection poolers

`LISTEN` needs a stable, dedicated backend, which is why `ergon_listener`'s
connection sits outside the pool. But if the `PG*` environment points at a
transaction-mode pooler such as PgBouncer, that dedicated connection still goes
through the pooler and `LISTEN` silently never receives anything. Point Ergon at
a session-mode route when a transaction-mode pooler sits in front of PostgreSQL.
""".

-behaviour(gen_server).

-export([start_link/0, start_link/1, channel/0, enabled/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include_lib("kernel/include/logger.hrl").

-define(CHANNEL, ~"ergon_job_available").

%% Resubscribe backoff, in milliseconds. The floor is what the delay used to be
%% flat at, so a single restarted listener is picked up just as quickly; the
%% ceiling is a reconnect ceiling and has nothing to do with a job's.
-define(RESUBSCRIBE_MIN, 500).
-define(RESUBSCRIBE_MAX, 30_000).

-type opts() :: #{config => pgo:pool_config()}.

-doc "The single `LISTEN`/`NOTIFY` channel job wake-ups travel on.".
-spec channel() -> binary().
channel() -> ?CHANNEL.

-doc "Whether the host has left the reactive fast path enabled. Defaults to true.".
-spec enabled() -> boolean().
enabled() ->
    proplists:get_value(enabled, application:get_env(ergon, ?MODULE, []), true).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() -> start_link(#{}).

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

-spec init(opts()) -> {ok, map()}.
init(_Opts) ->
    proc_lib:set_label(?MODULE),
    {ok, SubRef, MonRef} = ergon_listener:subscribe(?CHANNEL),
    {ok, #{sub => SubRef, mon => MonRef, backoff => new_backoff()}}.

%% `backoff:type/2` seeds this process's `rand` state as a side effect, so it has
%% to be called here rather than lazily on the first failure.
new_backoff() ->
    backoff:type(backoff:init(?RESUBSCRIBE_MIN, ?RESUBSCRIBE_MAX), jitter).

%% Payload is the queue name. Waking a queue this node does not drain is a no-op,
%% which is the common case: the tick reports every queue with runnable work,
%% not only the ones present here.
handle_info({notification, _Pid, _Ref, ?CHANNEL, Queue}, State) ->
    ok = ergon_worker_registry:wake(Queue),
    {noreply, State};
%% The shared connection died. Reconnects and their re-LISTEN are handled inside
%% the driver, so reaching here means the driver process itself crashed and the
%% subscription table went with it. Its supervisor restarts it; re-subscribe
%% rather than crashing, since workers keep draining on their poll meanwhile.
handle_info({'DOWN', MonRef, process, _Pid, Reason}, #{mon := MonRef} = State) ->
    ?LOG_WARNING(#{at => listener_down, channel => ?CHANNEL, reason => Reason}),
    {noreply, resubscribe(State)};
handle_info(resubscribe, State) ->
    {noreply, resubscribe(State)};
handle_info(_Msg, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

%% The connection is restarted by its own supervisor, which may not have happened
%% yet when the DOWN arrives, so this retries rather than assuming it is back.
%%
%% The retry is a jittered exponential rather than the flat 500 ms it used to be.
%% What the database going away looks like from here is every notifier on every
%% node arriving at this clause within the same instant, and a fixed delay keeps
%% them in that formation indefinitely: the same two attempts per second, in
%% lockstep, for as long as the outage lasts. `backoff` in `jitter` mode both
%% spreads the retries and stops escalating at the ceiling, drawing uniformly
%% from `[max/3, max]` there instead of pinning every node to the same value.
%% See `ergon.retry_backoff` for the article all of this comes from; the variant
%% here is its Decorrelated Jitter, which is what a caller carrying state across
%% attempts can afford and a job row cannot.
resubscribe(#{backoff := Backoff} = State) ->
    case whereis(ergon_listener:name()) of
        undefined ->
            {Delay, Backoff1} = backoff:fail(Backoff),
            _ = erlang:send_after(Delay, self(), resubscribe),
            State#{backoff := Backoff1};
        _Pid ->
            {ok, SubRef, MonRef} = ergon_listener:subscribe(?CHANNEL),
            {_Delay, Backoff1} = backoff:succeed(Backoff),
            State#{sub := SubRef, mon := MonRef, backoff := Backoff1}
    end.
