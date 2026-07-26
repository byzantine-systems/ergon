# Workflows and DAG dependencies

Ergon resolves job dependencies with a PostgreSQL SQL/PGQ **property graph** (PG
19), not recursive CTEs or an application-side scheduler. Jobs are vertices,
dependency edges connect them, and "what is ready to run?" is a single
`GRAPH_TABLE`/`MATCH` query.

## Declaring a dependency

`Ergon.depends_on/2` adds a `triggers` edge: the child runs only after the parent
completes.

```elixir
{:ok, build}  = Ergon.enqueue(Ergon.NewJob.new("build"))
{:ok, deploy} = Ergon.enqueue(Ergon.NewJob.new("deploy"))

:ok = Ergon.depends_on(build.id, deploy.id)
```

Until `build` completes, `deploy` is blocked and won't appear as ready:

```elixir
# deploy is not ready yet, build hasn't completed:
{:ok, []} = Ergon.ready_children()
```

## Asking what is ready

Two queries answer the scheduling question from different angles:

```elixir
# Every job whose parents have all completed, across the whole graph:
{:ok, ids} = Ergon.ready_children()

# The jobs a specific completed parent directly unblocks:
{:ok, ids} = Ergon.unblocked_by(build.id)
```

`ready_children/0` is the graph-wide view, a worker loop can drain it to pick up
newly runnable work. `unblocked_by/1` is the local view, useful right after a
job completes, to see exactly what it freed.

## Fan-out and fan-in

Edges compose into arbitrary DAGs. A job is ready only when **all** of its
parents have completed, which gives you fan-in for free:

```elixir
{:ok, lint}    = Ergon.enqueue(Ergon.NewJob.new("lint"))
{:ok, test}    = Ergon.enqueue(Ergon.NewJob.new("test"))
{:ok, release} = Ergon.enqueue(Ergon.NewJob.new("release"))

# release waits for BOTH lint and test:
:ok = Ergon.depends_on(lint.id, release.id)
:ok = Ergon.depends_on(test.id, release.id)
```

## Labelled edges

`depends_on/2` is `triggers` by convention. For richer graphs, add your own edge
labels with `link/3`:

```elixir
:ok = Ergon.link(parent.id, child.id, "triggers")
:ok = Ergon.link(parent.id, child.id, "notifies")
```

## Cancelling a subtree

Cancelling a job cascades to every descendant still running or waiting, and
returns the jobs actually discarded:

```elixir
{:ok, discarded} = Ergon.cancel(build.id)
# build plus everything downstream that hadn't finished
```

This is a graph traversal in the database, so a deep dependency tree is torn down
in one call rather than N round-trips.

## How to model the schema

The graph is built from vertex and edge tables. Ergon ships its own for
`ergon.jobs`, but you can build parallel graphs over your domain tables with the
[migration helpers](migrations.md) (`vertex_table/2`, `edge_table/4`).
