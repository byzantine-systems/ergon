-module(ergon).
-moduledoc """
PostgreSQL-native background job and workflow processing.

This is the module a host application uses. It covers the whole lifecycle:
enqueue jobs, wire up workflow dependencies, ask what the graph is doing, and
start workers to execute them.

```erlang
{ok, Job} = ergon:enqueue(
              ergon_new_job:on_queue(
                ergon_new_job:new(~"send_email", #{~"to" => ~"a@b.com"}),
                ~"mailers")),

{ok, _Worker} = ergon:start_worker(
                  ergon_queue:with_concurrency(ergon_queue:new(~"mailers"), 4),
                  fun handle_email/1).
```

The schema is installed by `ergon_migrate`, and the connection pool starts with
the application.

## Where the work actually happens

Almost none of it is here. Uniqueness, retry backoff, state-transition legality,
history, and dependency blocking are all enforced by PostgreSQL. See
`ergon_db`, and the migrations under `priv/migrations`. This module and the
worker processes are the thin part.

One consequence worth stating plainly: **a job with incomplete parents is not
checked out.** `depends_on/2` is a scheduling constraint, not an annotation. A
parent that ends `failed` or `discarded` therefore leaves its children blocked
until an operator intervenes, which is the honest behaviour for a dependency.
`cancel/1` exists to tear a stuck subtree down deliberately.
""".

-include_lib("ergon/include/ergon.hrl").

-export([
    enqueue/1,
    depends_on/2,
    link/3,
    ready_children/0,
    unblocked_by/1,
    cancel/1,
    start_worker/2,
    stop_worker/1,
    start_consumer/2,
    stop_consumer/1
]).

-doc """
Enqueue a job and return the inserted row.

## Enqueuing inside your own transaction

This is an ordinary query on Ergon's pool, so calling it inside
`ergon_repo:transaction/1` enlists it in that transaction: `pgo` binds the
transaction's connection in the process dictionary and every nested query rides
it. A job and the rows that justify it therefore commit or roll back together.

```erlang
ergon_repo:transaction(fun() ->
    {ok, _} = ergon_repo:query("UPDATE orders SET status = 'paid' WHERE id = $1", [OrderId]),
    {ok, Job} = ergon:enqueue(ergon_new_job:new(~"send_receipt", #{~"order" => OrderId}))
end).
```

There is no window in which the order was marked paid but the receipt job was
lost, and none in which the job runs for an order whose update rolled back. This
is the transactional outbox pattern without the outbox: the table, the poller and
the drift reconciler that pattern needs all exist to close a gap that does not
open when the queue lives in the same database as the data.

The same holds for `depends_on/2` and `link/3`, so an entire workflow can be
declared atomically with the business change that motivates it.
""".
-spec enqueue(new_job()) -> {ok, job()} | {error, db_error()}.
enqueue(NewJob) -> ergon_db:insert(NewJob).

-doc """
Declare that `Parent` completing should release `Child`, adding a `triggers`
edge.

Until `Parent` reaches `completed`, `Child` is withheld from checkout. Rejected
with `{error, would_create_cycle}` if the edge would close a loop.
""".
-spec depends_on(ergon_job:job_id(), ergon_job:job_id()) -> ok | {error, db_error()}.
depends_on(Parent, Child) -> ergon_db:link(Parent, Child).

-doc "Add a labelled dependency edge to the workflow graph.".
-spec link(ergon_job:job_id(), ergon_job:job_id(), binary()) -> ok | {error, db_error()}.
link(Parent, Child, EdgeType) -> ergon_db:link(Parent, Child, EdgeType).

-doc """
The ids of every job whose workflow parents have all completed.

Observability, not scheduling: workers pick these up on their own, because
checkout already excludes anything still blocked.
""".
-spec ready_children() -> {ok, [ergon_job:job_id()]} | {error, db_error()}.
ready_children() -> ergon_graph:ready_children().

-doc "The ids of the available jobs a completed `Parent` directly unblocks.".
-spec unblocked_by(ergon_job:job_id()) -> {ok, [ergon_job:job_id()]} | {error, db_error()}.
unblocked_by(Parent) -> ergon_graph:direct_children(Parent).

-doc """
Cancel `Job` and cascade to every descendant still running or waiting, returning
the jobs actually discarded.

Terminal descendants are left alone. This is the way to clear a subtree blocked
behind a parent that will never complete.
""".
-spec cancel(ergon_job:job_id()) -> {ok, [job()]} | {error, db_error()}.
cancel(Job) -> ergon_db:cancel_cascade(Job).

-doc """
Start a supervised worker that drains `Queue`, running `Handler` on each job.

Returns the pid of the queue's supervisor, for `stop_worker/1`.
""".
-spec start_worker(queue(), handler()) -> {ok, pid()} | {error, term()}.
start_worker(Queue, Handler) -> ergon_worker_sup:start_worker(Queue, Handler).

-doc "Stop a worker started by `start_worker/2`, along with its executor pool.".
-spec stop_worker(pid()) -> ok | {error, not_found}.
stop_worker(Worker) -> ergon_worker_sup:stop_worker(Worker).

-doc """
Start a supervised consumer draining a pgmq queue.

The other queue. `ergon.jobs` is for work with retries, dependencies and history;
pgmq is a durable message transport for streaming at volume, with at-least-once
delivery from visibility timeouts rather than state transitions. Configure it
with `ergon_pgmq_queue`, create the queue itself with
`ergon_pgmq:create_queue/1`.

```erlang
{ok, _} = ergon:start_consumer(
            ergon_pgmq_queue:with_concurrency(ergon_pgmq_queue:new(~"events"), 8),
            fun handle_event/1).
```
""".
-spec start_consumer(pgmq_queue(), pgmq_handler()) -> {ok, pid()} | {error, term()}.
start_consumer(Queue, Handler) -> ergon_pgmq_consumer_sup:start_consumer(Queue, Handler).

-doc "Stop a consumer started by `start_consumer/2`, along with its pools.".
-spec stop_consumer(pid()) -> ok | {error, not_found}.
stop_consumer(Consumer) -> ergon_pgmq_consumer_sup:stop_consumer(Consumer).
