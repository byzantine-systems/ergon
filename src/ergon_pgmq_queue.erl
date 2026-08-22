-module(ergon_pgmq_queue).
-moduledoc """
Runtime configuration for one pgmq consumer. Mirrors `ergon_queue`, so both
kinds of queue are configured the same way.

```erlang
Q = ergon_pgmq_queue:with_concurrency(
      ergon_pgmq_queue:with_notify(ergon_pgmq_queue:new(~"events")), 4),
{ok, _} = ergon:start_consumer(Q, fun handle/1).
```

## Choosing a wake path

Three ways to find out a message has arrived, in increasing order of what they
cost the database:

- **Poll only** (the default). `poll_interval` between reads, 100 ms out of the
  box. Latency is the interval; the price is one query per interval per consumer
  even when the queue is empty.
- **Notify** (`with_notify/1,2`). Ergon's single `ergon.notify_pending_pgmq()`
  tick wakes the consumer, so `poll_interval` can be raised to seconds without
  losing responsiveness. Latency floor is the tick's 1 s, which is pg_cron's
  finest granularity. One shared `LISTEN` connection serves every consumer on
  the node.
- **Long poll** (`with_long_poll/1,2,3`). The read itself blocks server-side
  until a message arrives. Lowest latency (100 ms by default) *and* no repeated
  round-trips, but the connection is held for the whole call, so the consumer
  gets a single-connection pool of its own.

Long poll is the better choice for a handful of latency-sensitive queues; notify
is the better choice for many mostly-idle ones, since it costs one connection
per node rather than one per consumer.
""".

-include_lib("ergon/include/ergon.hrl").

-export([
    new/1,
    with_poll_interval/2,
    with_batch_size/2,
    with_visibility_timeout/2,
    with_concurrency/2,
    with_handler_timeout/2,
    with_notify/1, with_notify/2,
    with_long_poll/1, with_long_poll/2, with_long_poll/3,
    with_read_strategy/2,
    long_poll/1,
    effective_handler_timeout/1
]).

-export_type([pgmq_queue/0]).

-define(DEFAULT_POLL_INTERVAL, 100).
-define(DEFAULT_BATCH_SIZE, 10).
-define(DEFAULT_VISIBILITY_TIMEOUT, 30).
-define(DEFAULT_CONCURRENCY, 1).
-define(DEFAULT_LONG_POLL_SECONDS, 5).
-define(DEFAULT_LONG_POLL_INTERVAL, 100).

-doc """
A consumer for `Name`: polled every 100 ms, ten messages per read, a 30 s
visibility timeout, one handler at a time, and no wake path beyond the poll.

`handler_timeout` is left `undefined` here and resolves to the visibility
timeout unless set explicitly. See `effective_handler_timeout/1`.
""".
-spec new(binary()) -> pgmq_queue().
new(Name) when is_binary(Name) ->
    #{
        name => Name,
        poll_interval => ?DEFAULT_POLL_INTERVAL,
        batch_size => ?DEFAULT_BATCH_SIZE,
        visibility_timeout => ?DEFAULT_VISIBILITY_TIMEOUT,
        concurrency => ?DEFAULT_CONCURRENCY,
        handler_timeout => undefined,
        notify_channel => undefined,
        read_strategy => plain
    }.

-doc "Set how long the consumer waits between reads, in milliseconds.".
-spec with_poll_interval(pgmq_queue(), pos_integer()) -> pgmq_queue().
with_poll_interval(Queue, Milliseconds) when is_integer(Milliseconds), Milliseconds > 0 ->
    Queue#{poll_interval := Milliseconds}.

-doc """
Set how many messages one read takes.

Also the width of a batch cycle: the consumer dispatches the whole batch, waits
for all of it, and archives the successes in one call. Raising it amortises
round-trips; it does not raise concurrency, which is `with_concurrency/2`.
""".
-spec with_batch_size(pgmq_queue(), pos_integer()) -> pgmq_queue().
with_batch_size(Queue, BatchSize) when is_integer(BatchSize), BatchSize > 0 ->
    Queue#{batch_size := BatchSize}.

-doc """
Set how long a read message stays hidden, in seconds.

This is the redelivery clock and therefore the failure-handling mechanism: a
message not archived within it becomes visible again and is delivered to someone
else. It should comfortably exceed how long a handler takes, or work will be
duplicated rather than retried.
""".
-spec with_visibility_timeout(pgmq_queue(), pos_integer()) -> pgmq_queue().
with_visibility_timeout(Queue, Seconds) when is_integer(Seconds), Seconds > 0 ->
    Queue#{visibility_timeout := Seconds}.

-doc "Set how many handlers may run at once. Sizes the executor pool.".
-spec with_concurrency(pgmq_queue(), pos_integer()) -> pgmq_queue().
with_concurrency(Queue, Concurrency) when is_integer(Concurrency), Concurrency > 0 ->
    Queue#{concurrency := Concurrency}.

-doc """
Set a per-message deadline in milliseconds, after which the handler is killed.

Rarely needed: the default is the visibility timeout, which is already the right
answer. Past that point pgmq has made the message visible again and another
consumer may hold it, so continuing is pointless and archiving afterwards would
remove a message someone else is still working.
""".
-spec with_handler_timeout(pgmq_queue(), timeout()) -> pgmq_queue().
with_handler_timeout(Queue, infinity) ->
    Queue#{handler_timeout := infinity};
with_handler_timeout(Queue, Milliseconds) when is_integer(Milliseconds), Milliseconds > 0 ->
    Queue#{handler_timeout := Milliseconds}.

-doc """
Wake this consumer through Ergon's notification tick, on the default channel
`pgmq_<queue>`.

Register the queue with `ergon_pgmq:enable_notify/1` as well; this side only
subscribes. Pair it with a longer `with_poll_interval/2`, because the point is to stop
polling an idle queue, not to poll it at 100 ms *and* listen.
""".
-spec with_notify(pgmq_queue()) -> pgmq_queue().
with_notify(#{name := Name} = Queue) -> with_notify(Queue, ergon_pgmq:default_channel(Name)).

-doc "Like `with_notify/1` on an explicit channel.".
-spec with_notify(pgmq_queue(), binary()) -> pgmq_queue().
with_notify(Queue, Channel) when is_binary(Channel) ->
    Queue#{notify_channel := Channel}.

-doc """
Block server-side on each read until a message arrives, for up to 5 seconds,
checking every 100 ms.

Gives the consumer a single-connection pool of its own, because the call holds
its connection for the whole block. Do not combine with `with_notify/1`: the read
already returns the instant work appears, so a notification has nothing to add.
""".
-spec with_long_poll(pgmq_queue()) -> pgmq_queue().
with_long_poll(Queue) -> with_long_poll(Queue, ?DEFAULT_LONG_POLL_SECONDS).

-doc "Like `with_long_poll/1` with an explicit block, in seconds.".
-spec with_long_poll(pgmq_queue(), pos_integer()) -> pgmq_queue().
with_long_poll(Queue, MaxSeconds) ->
    with_long_poll(Queue, MaxSeconds, ?DEFAULT_LONG_POLL_INTERVAL).

-doc """
Like `with_long_poll/2` with an explicit server-side check interval, in
milliseconds. That interval is the latency floor.

Preserves any grouped strategy already set, so ordering and long polling
compose.
""".
-spec with_long_poll(pgmq_queue(), pos_integer(), pos_integer()) -> pgmq_queue().
with_long_poll(#{read_strategy := Strategy} = Queue, MaxSeconds, IntervalMs) when
    is_integer(MaxSeconds), MaxSeconds > 0, is_integer(IntervalMs), IntervalMs > 0
->
    Queue#{read_strategy := {long_poll, base_strategy(Strategy), MaxSeconds, IntervalMs}}.

-doc """
Set the read strategy, i.e. whether and how messages are ordered.

`plain` takes whatever is visible. `grouped_head`, `grouped_rr` and `grouped`
order strictly within each `x-pgmq-group` while letting different groups run in
parallel. See `pgmq_read_strategy()`. Send with `ergon_pgmq:group_header/1` to
put a message in a group.

Preserves any long-poll setting, so the two compose in either order.
""".
-spec with_read_strategy(pgmq_queue(), plain | grouped | grouped_head | grouped_rr) ->
    pgmq_queue().
with_read_strategy(#{read_strategy := {long_poll, _Base, Max, Interval}} = Queue, Strategy) ->
    Queue#{read_strategy := {long_poll, Strategy, Max, Interval}};
with_read_strategy(Queue, Strategy) ->
    Queue#{read_strategy := Strategy}.

-doc "Whether this consumer blocks server-side on read, and so needs its own pool.".
-spec long_poll(pgmq_queue()) -> boolean().
long_poll(#{read_strategy := {long_poll, _, _, _}}) -> true;
long_poll(#{read_strategy := {long_poll, _, _}}) -> true;
long_poll(_Queue) -> false.

-doc """
The handler deadline actually applied: the configured one, or the visibility
timeout when none was set.
""".
-spec effective_handler_timeout(pgmq_queue()) -> timeout().
effective_handler_timeout(#{handler_timeout := undefined, visibility_timeout := Vt}) ->
    timer:seconds(Vt);
effective_handler_timeout(#{handler_timeout := Timeout}) ->
    Timeout.

base_strategy({long_poll, Base, _Max, _Interval}) -> Base;
base_strategy({long_poll, _Max, _Interval}) -> plain;
base_strategy(Strategy) -> Strategy.
