-module(ergon_queue).
-moduledoc """
Runtime configuration for a single named queue: how often a worker polls it and
how many jobs it takes per poll. Build with `new/1` plus the setters so call
sites are unaffected when a new tuning knob is added.

The default `poll_interval` is 5 s. With `ergon_job_notifier` running and pg_cron
installed, a runnable job wakes its workers over `LISTEN`/`NOTIFY` within the ~1 s
notifier tick, so the poll is only the fallback that covers the boot gap and
reconnect windows. Where pg_cron is absent no `NOTIFY` ever fires and the poll is
the *only* wake path, which is why the default stays moderate rather than long.
Raise it with `with_poll_interval/2` on pg_cron-backed deployments, or lower it
if the notifier is disabled and latency has to come from polling alone.
""".

-include_lib("ergon/include/ergon.hrl").

-export([
    new/1,
    with_poll_interval/2,
    with_batch_size/2,
    with_concurrency/2,
    with_handler_timeout/2
]).

-export_type([queue/0]).

-define(DEFAULT_POLL_INTERVAL, 5_000).
-define(DEFAULT_BATCH_SIZE, 1).
-define(DEFAULT_CONCURRENCY, 1).
-define(DEFAULT_HANDLER_TIMEOUT, infinity).

-doc """
A queue named `Name`: polled every 5 s, one job per checkout, one handler at a
time, and no handler deadline.

The defaults make a worker behave as a single sequential drain, so raising
`concurrency` is an explicit opt-in rather than something a host discovers by
surprise.
""".
-spec new(binary()) -> queue().
new(Name) when is_binary(Name) ->
    #{
        name => Name,
        poll_interval => ?DEFAULT_POLL_INTERVAL,
        batch_size => ?DEFAULT_BATCH_SIZE,
        concurrency => ?DEFAULT_CONCURRENCY,
        handler_timeout => ?DEFAULT_HANDLER_TIMEOUT
    }.

-doc "Set how long the worker waits between polls, in milliseconds.".
-spec with_poll_interval(queue(), pos_integer()) -> queue().
with_poll_interval(Queue, Milliseconds) when is_integer(Milliseconds), Milliseconds > 0 ->
    Queue#{poll_interval := Milliseconds}.

-doc """
Set the maximum number of jobs checked out per poll.

This is a round-trip optimisation, not a concurrency setting: a poll fetches at
most this many, and never more than the executor pool has free capacity for.
""".
-spec with_batch_size(queue(), pos_integer()) -> queue().
with_batch_size(Queue, BatchSize) when is_integer(BatchSize), BatchSize > 0 ->
    Queue#{batch_size := BatchSize}.

-doc """
Set how many of this queue's handlers may run at once. Sizes the executor pool.

Raising this is the in-process way to add throughput. Starting several workers on
the same queue, or on several nodes, is the other, and the two compose: checkout
uses `FOR UPDATE SKIP LOCKED`, so no two executors anywhere can take the same job.
""".
-spec with_concurrency(queue(), pos_integer()) -> queue().
with_concurrency(Queue, Concurrency) when is_integer(Concurrency), Concurrency > 0 ->
    Queue#{concurrency := Concurrency}.

-doc """
Set a per-job deadline, in milliseconds, after which a running handler is killed.

The job is then recorded as errored rather than abandoned, so it consumes an
attempt and the database's jittered backoff reschedules it. Without a deadline
(the `infinity` default) a handler that hangs occupies an executor slot until the
node restarts, and its job stays `executing` for the reconciler to find.
""".
-spec with_handler_timeout(queue(), timeout()) -> queue().
with_handler_timeout(Queue, infinity) ->
    Queue#{handler_timeout := infinity};
with_handler_timeout(Queue, Milliseconds) when is_integer(Milliseconds), Milliseconds > 0 ->
    Queue#{handler_timeout := Milliseconds}.
