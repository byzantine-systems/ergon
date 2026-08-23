# Getting Started

The job path end to end: enqueue, work, retry, and commit atomically with your own data.

## Enqueue a Job

A job is built with `ergon_new_job` and inserted with `ergon:enqueue/1`:

```erlang
{ok, Job} = ergon:enqueue(ergon_new_job:new(~"send_email", #{~"to" => ~"a@b.com"})).
```

`new/2` takes a worker name and a payload. The payload is any term `json:encode/1` accepts, and it comes back decoded, so a handler sees the shape it enqueued rather than JSON text.

The setters are pipeline-friendly in the sense that each returns the job, so a call site is unaffected when a new knob is added:

```erlang
Job = ergon_new_job:unique_for(
        ergon_new_job:with_max_attempts(
          ergon_new_job:on_queue(
            ergon_new_job:new(~"send_email", #{~"to" => ~"a@b.com"}),
            ~"mailers"),
          5),
        60).
```

Defaults: the `default` queue, 20 attempts, not unique.

## Running a Worker

```erlang
{ok, Worker} = ergon:start_worker(ergon_queue:new(~"mailers"), fun handle/1).

handle(#{payload := #{~"to" := To}}) ->
    case my_mailer:send(To) of
        ok -> ok;
        {error, Reason} -> {error, atom_to_binary(Reason)}
    end.
```

The handler contract is exactly three outcomes:

- `ok` completes the job.
- `{error, Binary}` records the reason and retries until attempts are spent.
- Anything else, including a `raise`, a `throw`, or an `exit`, is treated as an `error`. One bad job never takes a worker down.

`ergon:stop_worker/1` takes the pid returned above and stops the worker with its executor pool.

## Tuning the Queue

```erlang
Queue = 
    ergon_queue:with_handler_timeout(
        ergon_queue:with_concurrency(
            ergon_queue:with_batch_size(
              ergon_queue:with_poll_interval(ergon_queue:new(~"mailers"), 1000),
              10),
            4),
        30_000).
```

| Setting | Default | What it does |
|---|---|---|
| `poll_interval` | 5000 ms | how often the worker checks for work |
| `batch_size` | 1 | how many jobs one checkout fetches |
| `concurrency` | 1 | how many handlers may run at once |
| `handler_timeout` | `infinity` | per-job deadline, after which the handler is killed |

`batch_size` and `concurrency` are easy to conflate and are not the same thing:
- `batch_size` amortises database round-trips.
- `concurrency` sizes the executor pool. A poll never fetches more than the pool has free capacity for, so a checked-out job is never left waiting in a mailbox.

`handler_timeout` is worth setting. Without it, a handler that hangs occupies an executor slot until the node restarts, and its job stays `executing` for the reconciler to find. With it, the handler is killed and the job is recorded as errored, so it consumes an attempt and the database's backoff reschedules it.

## Retries and Backoff

Backoff is computed in SQL, not in Erlang, by `ergon.retry_backoff`. Each attempt doubles a ceiling, starting at 1 second and stopping at 100, and the delay is drawn at random from below it. So a job's first retry lands somewhere in the first second, its second somewhere in the first two, and so on. When attempts are exhausted the job becomes `failed` and stays there.

The randomness is the part that matters. Without it every job that fails in the same second is rescheduled to the same instant, so a batch that fails together against a struggling downstream service retries together, fails together, and re-synchronises on a longer interval. Past the point where the ceiling stops growing they stay aligned indefinitely. This is the problem, and the fix, described in Marc Brooker's [Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/); Ergon's default is the strategy it calls Full Jitter.

Three strategies ship, configured under `ergon_db`:

```erlang
{ergon, [{ergon_db, [{retry_backoff, [
    {strategy, full_jitter},   % random(0, ceiling). The default.
    {base_ms, 1000},           % the first retry's ceiling
    {cap_ms, 100000}           % where the ceiling stops doubling
]}]}]}
```

`equal_jitter` draws from `[ceiling/2, ceiling]`, trading some of the dispersion for a guaranteed floor. `none` is the unjittered ceiling itself, which is worth having when a test needs the schedule to be reproducible, and is otherwise the behaviour to avoid.

One consequence of jitter is worth planning for: a job's *expected* total wait across its retries is about half what the ceilings add up to, since each delay averages half its ceiling. If you sized something around the old schedule, double `base_ms`.

The attempt counter is incremented by the checkout statement itself, in the same statement that takes the row lock. That is what makes checkout atomic across concurrent workers, and it means an attempt is consumed even if the node dies before the handler runs.

## Concurrency

Two ways to add throughput, and they compose:

```erlang
%% in-process: one worker, four handlers at a time
ergon:start_worker(ergon_queue:with_concurrency(ergon_queue:new(~"mailers"), 4), Handler)

%% across processes or nodes: start more workers on the same queue
[ergon:start_worker(ergon_queue:new(~"mailers"), Handler) || _ <- lists:seq(1, 3)]
```

Checkout uses `FOR UPDATE SKIP LOCKED`, so no two workers anywhere can take the same job, and no coordination is needed between them. Adding a node changes throughput and nothing else.

## Enqueue inside a transaction

Because a job is a row in your database, enqueuing inside a transaction enlists it in that transaction:

```erlang
ergon_repo:transaction(fun() ->
    {ok, _} = ergon_repo:query("UPDATE orders SET status = 'paid' WHERE id = $1", [OrderId]),
    {ok, _} = ergon:enqueue(ergon_new_job:new(~"send_receipt", #{~"order" => OrderId}))
end).
```

Either the order is paid and the receipt job exists, or neither happened. This is the transactional outbox pattern with no outbox: the table, the poller, and the drift reconciler that pattern requires all exist to close a gap that does not open when the queue lives in the same database as the data.

The same holds for `ergon:depends_on/2` and `ergon:link/3`, so an entire workflow can be declared atomically with the change that motivates it.

## Waking up

A worker polls on its interval, and that poll alone is correct. On top of it, if pg_cron is installed, a one-second tick notifies queues that have runnable work and the worker drains immediately instead of waiting out its interval.

That means `poll_interval` can be raised to seconds without hurting latency, which is the point of it. A dropped notification costs latency, never a job.

## Next

- [Unique jobs](unique-jobs.md) for deduplication.
- [Workflows](workflows.md) for dependencies between jobs.
- [Operations](operations.md) for health checks and recovery.
