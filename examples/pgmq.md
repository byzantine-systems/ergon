# `pgmq`: durable message streaming

Ergon's *other* queue, and it is worth being clear which is which.

`ergon.jobs` is the bi-temporal job table: retries, workflow dependencies, history, one row per job. **pgmq** is a durable message transport. A host creates its own queues on it and streams through them at volume, and delivery is at-least-once by virtue of visibility timeouts rather than state transitions.

Nothing in this guide touches `ergon.jobs`.

## Create a queue and consume it

```erlang
ok = ergon_pgmq:create_queue(~"events"),

{ok, _Consumer} =
    ergon:start_consumer(
      ergon_pgmq_queue:with_concurrency(ergon_pgmq_queue:new(~"events"), 8),
      fun(#{message := #{~"kind" := Kind}}) ->
          my_app:handle(Kind)
      end).
```

Send from anywhere:

```erlang
{ok, MsgId} = ergon_pgmq:send(~"events", #{~"kind" => ~"signup"}).
```

## The delivery contract

**Archive means acknowledge.** A message is removed from the queue only after its handler returned `ok`. A failure is reported by *not* archiving: the visibility timeout expires and `pgmq` redelivers. There is no negative acknowledgement to send, and nothing deletes a message outright.

That is what makes delivery survive a crash mid-batch. Whatever was in flight was never archived, so it comes back. The cost is that redelivery is at-least-once, so **handlers must tolerate seeing a message twice**.

## How a consumer works

One batch at a time: read a batch, hand every message to the executor pool, wait for all of them, archive the successes in a single call, repeat. A full batch means there is probably more waiting, so the next cycle starts immediately instead of sleeping out the poll interval.

The cycle is its own backpressure. Exactly one batch is ever outstanding, and nothing is read until the last one is accounted for.

Two consequences worth knowing:

- One slow message delays its siblings' archive, since the cycle waits for the whole batch. Bounded by `handler_timeout`, which defaults to the visibility timeout.
- A message whose handler always fails is redelivered forever. `pgmq` has no dead-letter queue. `read_ct` counts deliveries and is the only signal a handler has to give up:

```erlang
handle(#{read_ct := Ct, message := M}) when Ct > 5 ->
    my_app:quarantine(M),
    ok;                       %% archive it so it stops coming back
handle(#{message := M}) ->
    my_app:process(M).
```

## Configuration

| Setting | Default | What it does |
|---|---|---|
| `poll_interval` | 100 ms | how often to read when idle |
| `batch_size` | 10 | messages per read, and the width of one cycle |
| `visibility_timeout` | 30 s | how long a read message stays hidden |
| `concurrency` | 1 | handlers running at once |
| `handler_timeout` | the visibility timeout | per-message deadline |

`handler_timeout` defaults to the visibility timeout rather than to `infinity`, unlike the job side, and the reason is worth stating: past that point pgmq has made the message visible again and another consumer may already hold it, so continuing is pointless and archiving afterwards would remove work someone else is doing.

## Three ways to wake a consumer

The poll is always underneath and always correct. On top of it:

```erlang
%% 1. poll only. Latency is the interval; an idle queue costs one query per
%%    interval.
ergon_pgmq_queue:new(~"events")

%% 2. notify. Ergon's one-second tick wakes the consumer, so the poll can be
%%    slowed right down. Register the queue as well as configuring the consumer.
ok = ergon_pgmq:enable_notify(~"events"),
ergon_pgmq_queue:with_notify(
  ergon_pgmq_queue:with_poll_interval(ergon_pgmq_queue:new(~"events"), 30_000))

%% 3. long poll. The read itself blocks server-side until a message arrives.
ergon_pgmq_queue:with_long_poll(ergon_pgmq_queue:new(~"events"), 5, 100)
```

Latency floors: notify is `pg_cron`'s **one second**, long poll is its check interval, **100 ms** by default. Long poll holds a connection while blocked, so a long-polling consumer gets a single-connection pool of its own rather than occupying the shared one.

Rule of thumb: long poll for a handful of latency-sensitive queues, notify for many mostly-idle ones, since notify costs one shared `LISTEN` connection per node rather than one per consumer. Do not combine them, the read already returns the moment work appears.

### Why the notification is a cron tick and not a trigger

Writers never call `pg_notify`. Every transaction with a pending `NOTIFY` takes the global notification-queue lock at commit, so a trigger firing per insert serialises every enqueuing commit. That caps write throughput regardless of hardware.

Ergon uses a **single** tick for all pgmq queues, reading a registry and emitting one notification per queue holding visible work, from one transaction. So the number of notifying transactions is one per second no matter how many queues exist.

## FIFO groups

Strict ordering within a group, parallelism across groups:

```erlang
{ok, _} = ergon_pgmq:send(~"events", Payload, ergon_pgmq:group_header(~"customer-42")),

Queue = ergon_pgmq_queue:with_read_strategy(
          ergon_pgmq_queue:new(~"events"), grouped_head).
```

| Strategy | Behaviour |
|---|---|
| `plain` | whatever is visible, unordered |
| `grouped_head` | at most one message per group, so no group is ever worked out of order |
| `grouped_rr` | round-robin across groups, so a busy group cannot starve others |
| `grouped` | batches from the earliest group, favouring throughput over fairness |

Ordering is enforced by the visibility timeout rather than by locking: the next message in a group stays hidden until the one ahead of it is archived or its lease expires. A message sent without the header joins a default group.

Grouping composes with long polling in either order.

## Topics

AMQP-style routing: bind patterns to queues, publish by routing key, and one message fans out to everything that matches.

```erlang
ok = ergon_pgmq:bind_topic(~"logs.#", ~"all_logs"),
ok = ergon_pgmq:bind_topic(~"logs.error", ~"error_logs"),

{ok, 2} = ergon_pgmq:send_topic(~"logs.error", #{~"m" => ~"boom"}),
{ok, 1} = ergon_pgmq:send_topic(~"logs.info", #{~"m" => ~"hi"}),
{ok, 0} = ergon_pgmq:send_topic(~"metrics.cpu", #{~"m" => ~"none"}).
```

`*` matches exactly one dot-separated segment, `#` matches zero or more. So `logs.#` catches both `logs.error` and `logs.api.error`, while `logs.*` catches only the first.

- A routing key nothing is bound to is silently dropped, which is what a topic exchange is supposed to do and also an easy way to lose messages to a typo. Check the count, or `ergon_pgmq:list_topic_bindings/1`, when delivery matters.
- The fan-out is one transaction: every delivery succeeds or none does.

## Transactional send

Like the job side, a send inside your own transaction commits with your data:

```erlang
ergon_repo:transaction(fun() ->
    {ok, _} = ergon_repo:query("UPDATE accounts SET balance = balance - $1 WHERE id = $2",
                               [Amount, From]),
    {ok, _} = ergon_pgmq:send(~"payments", #{~"from" => From, ~"amount" => Amount})
end).
```

No outbox table, no relay process, no reconciler for drift between the two. None of that machinery is needed when the queue is in the same database as the data.

## Operating a queue

```erlang
{ok, #{queue_length := Len, queue_visible_length := Visible}} =
    ergon_pgmq:metrics(~"events").
```

The gap between the two is messages hidden behind a visibility lease: either in flight, or stranded by a consumer that died. `ergon_reconciler` releases the stranded ones, see [operations](operations.md).

One caveat on `queue_visible_length`: it is computed against transaction-frozen `now()`, so a message sent inside the same transaction reads as invisible. Assert on `queue_length` in transactional tests.
