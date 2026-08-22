-module(ergon_reconciler).
-moduledoc """
Disaster recovery: put the runtime back in step with what the database says.

Run it after a node dies mid-flight, after a failover, or on any restart where
something may have been holding work when it stopped. Three moves, in this
order, and the order matters:

1. **The host's `hydrate` callback.** Ergon does not know what in-memory state a
   host keeps, so this is where the host stops suspect processes and rebuilds
   them. It runs *first* for a specific reason: releasing messages before
   stopping the consumers that will receive them just hands redelivered work to
   processes that are about to be killed.

2. **Release stranded pgmq leases.** A consumer that died mid-processing left its
   messages invisible until their visibility timeout expires, which may be
   thirty seconds or thirty minutes. `ergon_pgmq:release_leases/1` expires them
   all at once, then metrics are snapshotted so they reflect the recovered state
   rather than the broken one.

3. **Check `pending_parents` for drift.** Reported by default, repaired only when
   asked.

```erlang
ergon_reconciler:run(#{
    pgmq_queues => [~"events"],
    hydrate => fun my_app_state:stop_all_and_rebuild/0
}).
```

## Why the drift check exists

`ergon.jobs.pending_parents` counts a job's incomplete workflow parents, and it
is what `jobs_fetch_idx` carries in its predicate so that blocked jobs are absent
from the index rather than scanned past. That denormalisation is what keeps
checkout proportional to the batch size instead of to the blocked backlog, and it
is also what makes the counter unverifiable by any normal means: nothing reads
`job_edges` on the hot path any more.

So if the maintaining triggers have a bug, or someone writes the column by hand,
a job is left either permanently unrunnable (count too high) or running before
its parents finish (count too low). Neither surfaces anywhere. `drift/0`
recomputes the truth from `job_edges` and reports only what disagrees.

It doubles as a cross-check on the workflow graph: `ergon_graph:ready_children/0`
answers the same question through an entirely independent mechanism, a graph
match rather than a trigger-maintained counter, so persistent disagreement
between them means one of the two is wrong.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("ergon/include/ergon.hrl").

-export([run/0, run/1, drift/0, repair_drift/0]).

-export_type([summary/0, queue_stats/0, drift_row/0]).

-type queue_stats() :: #{
    released_leases := non_neg_integer(),
    queue_length := non_neg_integer(),
    queue_visible_length := non_neg_integer(),
    oldest_msg_age_sec := number() | pg_null()
}.

-type drift_row() :: #{
    id := ergon_job:job_id(),
    actual := non_neg_integer(),
    expected := non_neg_integer()
}.

-type summary() :: #{
    hydrate := term(),
    pgmq := #{binary() => queue_stats() | {error, db_error()}},
    pending_parents_drift := [drift_row()],
    repaired := [ergon_job:job_id()] | not_attempted
}.

-type opts() :: #{
    pgmq_queues => [binary()],
    hydrate => fun(() -> term()),
    repair => boolean()
}.

-doc "Run the recovery flow with defaults: no queues, no hydrate, no repair.".
-spec run() -> summary().
run() -> run(#{}).

-doc """
Run the recovery flow.

Options:

- `pgmq_queues => [Name]`, whose leases to release and snapshot. Empty by
  default, since Ergon cannot know which pgmq queues are the host's.
- `hydrate => fun/0`, the host's state rebuild. Defaults to a no-op, which is
  correct for a host that keeps no in-memory state.
- `repair => true`, rewrite `pending_parents` where it has drifted. **Off by
  default.** Repairing silently would hide whatever caused the drift, and the
  drift itself is the more useful signal.
""".
-spec run(opts()) -> summary().
run(Opts) ->
    Queues = maps:get(pgmq_queues, Opts, []),
    Hydrate = maps:get(hydrate, Opts, fun() -> ok end),
    Repair = maps:get(repair, Opts, false),

    %% Host state first. See the module docs: the ordering is the point.
    HydrateResult = Hydrate(),

    Pgmq = maps:from_list([{Q, recover_queue(Q)} || Q <- Queues]),
    Drift = drift(),
    Repaired =
        case {Repair, Drift} of
            {true, [_ | _]} -> repair_drift();
            {true, []} -> [];
            {false, _} -> not_attempted
        end,

    Summary = #{
        hydrate => HydrateResult,
        pgmq => Pgmq,
        pending_parents_drift => Drift,
        repaired => Repaired
    },
    log(Summary),
    Summary.

-doc """
Jobs whose `pending_parents` disagrees with `ergon.job_edges`. Empty when healthy.

Cheap enough to run on a schedule; the query returns only rows that disagree.
""".
-spec drift() -> [drift_row()].
drift() ->
    case ergon_sql:query({jobs, pending_parents_drift}, []) of
        {ok, #{rows := Rows}} ->
            [
                #{id => Id, actual => Actual, expected => Expected}
             || {Id, Actual, Expected} <:- Rows
            ];
        {error, Reason} ->
            ?LOG_WARNING(#{at => drift_check_failed, reason => Reason}),
            []
    end.

-doc """
Rewrite `pending_parents` from `ergon.job_edges` wherever it disagrees. Returns
the ids corrected.

Writes only the rows that are actually wrong, which matters because each one
fires the versioning trigger and accrues a history row.
""".
-spec repair_drift() -> [ergon_job:job_id()].
repair_drift() ->
    case ergon_sql:query({jobs, repair_pending_parents}, []) of
        {ok, #{rows := Rows}} ->
            [Id || {Id} <:- Rows];
        {error, Reason} ->
            ?LOG_WARNING(#{at => drift_repair_failed, reason => Reason}),
            []
    end.

%% ---------------
%% Helpers
%% ---------------

%% Release first, then snapshot, so the metrics describe the recovered queue
%% rather than the one that was broken a moment ago.
recover_queue(Queue) ->
    maybe
        {ok, Released} ?= ergon_pgmq:release_leases(Queue),
        {ok, Metrics} ?= ergon_pgmq:metrics(Queue),
        Metrics#{released_leases => Released}
    end.

log(#{hydrate := Hydrate, pgmq := Pgmq, pending_parents_drift := Drift, repaired := Repaired}) ->
    ?LOG_NOTICE(#{
        at => reconciled,
        hydrate => Hydrate,
        pgmq => maps:map(fun(_Q, Stats) -> summarise(Stats) end, Pgmq),
        drift => length(Drift),
        repaired => Repaired
    }).

summarise(#{queue_length := Length, released_leases := Released}) ->
    #{depth => Length, released_leases => Released};
summarise(Other) ->
    Other.
