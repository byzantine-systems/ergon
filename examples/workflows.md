# Workflows and DAG dependencies

Job dependencies that are enforced by checkout, not merely queryable.

## Declaring a dependency

```erlang
{ok, #{id := Build}}  = ergon:enqueue(ergon_new_job:new(~"build")),
{ok, #{id := Deploy}} = ergon:enqueue(ergon_new_job:new(~"deploy")),

ok = ergon:depends_on(Build, Deploy).
```

`deploy` is now **withheld from checkout** until `build` completes. Not merely absent from a readiness query: a worker draining that queue will not be handed it, and `ergon_db:checkout/2` will not return it.

```erlang
{ok, []} = ergon:ready_children(),   %% deploy is not ready
{ok, Jobs} = ergon_db:checkout(~"default", 10),
[~"build"] = [W || #{worker := W} <- Jobs].
```

Once `build` reaches `completed`, `deploy` becomes checkoutable on the next poll.

## Fan-out and fan-in

Edges compose into arbitrary DAGs, and a job is released only when **all** of its parents have completed:

```erlang
{ok, #{id := Lint}}    = ergon:enqueue(ergon_new_job:new(~"lint")),
{ok, #{id := Test}}    = ergon:enqueue(ergon_new_job:new(~"test")),
{ok, #{id := Release}} = ergon:enqueue(ergon_new_job:new(~"release")),

ok = ergon:depends_on(Lint, Release),
ok = ergon:depends_on(Test, Release).
```

## How the blocking works

Each job carries `pending_parents`, a count of its parents that have not completed, maintained by two triggers: one on `ergon.job_edges` for links added and removed, one on `ergon.jobs` for a parent reaching `completed`.

That count lives in the fetch index's predicate:

```sql
CREATE INDEX jobs_fetch_idx ON ergon.jobs (queue, scheduled_at)
WHERE state = 'available'
  AND upper(valid_period) = 'infinity'
  AND pending_parents = 0;
```

So a blocked job is not merely filtered out of checkout, it is **absent from the index**. That matters more than it sounds. Expressing the same rule as a join against `job_edges` leaves `LIMIT` bounding the output but not the scan, and the index walk has to step over every blocked job ahead of the first runnable one. 

## A failed parent blocks its children indefinitely

Only `completed` releases a child. A parent that ends `failed` or `discarded` never completes, so its children stay blocked until someone intervenes.

That is the honest reading of a dependency: the workflow is stuck and wants an operator, not a child that runs as though its prerequisite had succeeded. Two ways out:

```erlang
%% see what is stuck
#{jobs := #{~"default" := #{blocked := N}}} = ergon_health:check(),

%% tear the subtree down deliberately
{ok, Discarded} = ergon:cancel(Build).
```

`ergon:cancel/1` cascades to every descendant still in a cancellable state, discarding each through a proper valid-time transition so history is preserved. Terminal descendants are left alone.

## Asking what the graph is doing

Two queries, both observability rather than scheduling, since checkout already excludes anything blocked:

```erlang
%% every job whose parents have all completed
{ok, Ids} = ergon:ready_children(),

%% what a specific completed parent directly released
{ok, Ids} = ergon:unblocked_by(Build).
```

Both run over the `ergon.workflow` property graph with a single `GRAPH_TABLE` match. Multi-hop reachability, such as the cascade behind `cancel/1`, uses a recursive CTE instead: PG19's SQL/PGQ has no path quantifiers, so variable-length reachability cannot be expressed in `MATCH` at all.

`ready_children/0` is also a useful cross-check on `pending_parents`, because the two answer the same question through entirely independent mechanisms: a graph match against a trigger-maintained counter. Persistent disagreement means one of them is wrong, which is what `ergon_reconciler:drift/0` exists to detect.

## Cycles are rejected

```erlang
ok = ergon:depends_on(A, B),
{error, would_create_cycle} = ergon:depends_on(B, A),
{error, would_create_cycle} = ergon:depends_on(A, A).
```

The check runs in the database as a recursive reachability query, inside a transaction and behind a transaction-scoped advisory lock. The lock is not decoration: under READ COMMITTED two concurrent calls adding opposite edges would each evaluate reachability against a snapshot taken before the other's insert, both see no cycle, and both commit.

## Labelled edges

`depends_on/2` adds a `triggers` edge. For richer graphs, label your own:

```erlang
ok = ergon:link(Parent, Child, ~"triggers"),
ok = ergon:link(Parent, Child, ~"notifies").
```

Only `triggers` participates in blocking.
