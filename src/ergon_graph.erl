-module(ergon_graph).
-moduledoc """
Workflow dependency resolution over PostgreSQL 19's SQL/PGQ property graph.

The `ergon.workflow` graph maps jobs and edges into `job` vertices connected by
`triggers` edges, so "which jobs are ready to run?" is a single `GRAPH_TABLE`
match rather than a hand-rolled join. Its vertex table is `ergon.jobs_current`,
not `ergon.jobs`: under the temporal primary key a job owns one row per
valid-time version, and using the base table gave a completed parent one vertex
per version, which made `ready_children/0` return nothing for any parent that had
ever run. See the property graph comment in `priv/migrations/schema`.

## These are observability queries, not the scheduler

Dependencies are enforced in `checkout.sql` through `ergon.jobs.pending_parents`,
so a blocked job is simply not checked out and nothing here needs to be consulted
for correctness. `ready_children/0` and `direct_children/1` answer "what is the
workflow doing?", which is what a dashboard or an operator wants.

Multi-hop reachability uses a recursive CTE rather than a graph walk. PG19's
SQL/PGQ has no path quantifiers, so variable-length reachability cannot be
expressed in `MATCH` at all.
""".

-include_lib("ergon/include/ergon.hrl").

-export([
    ready_children/0,
    direct_children/1,
    descendants/1,
    would_create_cycle/2
]).

-doc """
The ids of every `available` job whose workflow parents have *all* completed.

Equivalently: the jobs whose `pending_parents` has just reached zero. The two
are maintained independently (one by triggers on the row, one by a graph match),
so a disagreement between them means the counter has drifted, which makes this a
useful cross-check for the reconciler as well as for a dashboard.
""".
-spec ready_children() -> {ok, [ergon_job:job_id()]} | {error, db_error()}.
ready_children() ->
    maybe
        {ok, #{rows := Rows}} ?= ergon_sql:query({graph, ready_children}, []),
        {ok, ids(Rows)}
    end.

-doc """
The ids of the `available` jobs a completed job directly unblocks: everything one
`triggers` hop away.
""".
-spec direct_children(ergon_job:job_id()) -> {ok, [ergon_job:job_id()]} | {error, db_error()}.
direct_children(ParentId) ->
    maybe
        {ok, #{rows := Rows}} ?= ergon_sql:query({graph, direct_children}, [ParentId]),
        {ok, ids(Rows)}
    end.

-doc """
Every job id reachable from `AncestorId` through `triggers` edges: its transitive
closure, and the set a cascade would touch.
""".
-spec descendants(ergon_job:job_id()) -> {ok, [ergon_job:job_id()]} | {error, db_error()}.
descendants(AncestorId) ->
    maybe
        {ok, #{rows := Rows}} ?= ergon_sql:query({graph, descendants}, [AncestorId]),
        {ok, ids(Rows)}
    end.

-doc """
Whether adding `ParentId -> ChildId` would introduce a cycle, including a
self-loop.

`ergon_db:link/3` runs this itself, inside a transaction and behind an advisory
lock, so calling it beforehand is advisory only: between this answer and a later
`link/3` another process may have added the edge that closes the loop.
""".
-spec would_create_cycle(ergon_job:job_id(), ergon_job:job_id()) ->
    {ok, boolean()} | {error, db_error()}.
would_create_cycle(ParentId, ChildId) ->
    maybe
        {ok, #{rows := [{Cycle}]}} ?=
            ergon_sql:query({graph, would_create_cycle}, [
                ParentId, ChildId
            ]),
        {ok, Cycle}
    end.

%% Strict generator. Every one of these queries projects exactly one column, so a
%% row that is not a 1-tuple means the query grew a column and the caller's
%% contract changed. With a relaxed `<-` that would silently yield [], which reads
%% as "no ready children": an empty workflow rather than a broken query.
ids(Rows) -> [Id || {Id} <:- Rows].
