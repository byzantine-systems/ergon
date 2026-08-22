-module(ergon_health).
-moduledoc """
Liveness and diagnostics, in one call.

`check/0` returns everything a `/health` endpoint needs:

- `db`, whether the pool can serve a trivial query.
- `extensions`, installed PostgreSQL extensions and their versions. Worth
  surfacing because Ergon's behaviour genuinely depends on them: without pg_cron
  no notification tick runs and every wake path falls back to polling.
- `jobs`, per-queue counts from `ergon.jobs`.
- `pgmq`, per-queue metrics for whichever pgmq queues were asked about.

```erlang
{ergon, [{ergon_health, [{pgmq_queues, [~"events", ~"receipts"]}]}]}
```

pgmq queues have to be named because pgmq has no notion of which queues belong
to this application, and reporting on every queue in the database would include
other applications'. Job queues need no such list: they are rows in
`ergon.jobs`, so the query finds them.

## Reading the job counts

`runnable` is what checkout would take next, and uses the same predicate as
`jobs_fetch_idx`. `blocked` is the one to watch: those jobs are available but
waiting on a workflow parent, so they are invisible to checkout, and a queue full
of them looks idle while holding work. Since a parent that ended `failed` or
`discarded` never completes, its children stay blocked until an operator
intervenes with `ergon:cancel/1`. Nothing else surfaces that.

A persistently high `executing` with no throughput means consumers died holding
jobs, which is what `ergon_reconciler` is for.
""".

-include_lib("ergon/include/ergon.hrl").

-export([check/0, check/1, pgmq_queues/0]).

-export_type([health/0, job_metrics/0]).

-type job_metrics() :: #{
    runnable := non_neg_integer(),
    blocked := non_neg_integer(),
    scheduled := non_neg_integer(),
    executing := non_neg_integer(),
    failed := non_neg_integer(),
    discarded := non_neg_integer()
}.

-type health() :: #{
    db := ok | {error, db_error()},
    extensions := #{binary() => binary()},
    jobs := #{binary() => job_metrics()},
    pgmq := #{binary() => pgmq_metrics() | {error, db_error()}}
}.

-doc "The health snapshot, using the configured pgmq queue list.".
-spec check() -> health().
check() -> check(#{}).

-doc """
Like `check/0`, overriding the pgmq queue list for this call with
`#{pgmq_queues => [...]}`.

Never raises. Every section reports its own failure, because a health check that
crashes when the database is down is answering the wrong question.
""".
-spec check(#{pgmq_queues => [binary()]}) -> health().
check(Opts) ->
    Queues = maps:get(pgmq_queues, Opts, pgmq_queues()),
    #{
        db => db(),
        extensions => extensions(),
        jobs => jobs(),
        pgmq => maps:from_list([{Q, pgmq_metrics(Q)} || Q <- Queues])
    }.

-doc "The pgmq queues configured for reporting.".
-spec pgmq_queues() -> [binary()].
pgmq_queues() ->
    proplists:get_value(pgmq_queues, application:get_env(ergon, ?MODULE, []), []).

%% ---------------
%% Sections
%% ---------------

db() ->
    case ergon_sql:query({system, healthcheck}, []) of
        {ok, #{rows := [{1}]}} -> ok;
        {ok, Other} -> {error, {unexpected_healthcheck_result, Other}};
        {error, _} = Error -> Error
    end.

extensions() ->
    case ergon_sql:query({system, installed_extensions}, []) of
        {ok, #{rows := Rows}} -> maps:from_list([{Name, Version} || {Name, Version} <:- Rows]);
        {error, _} -> #{}
    end.

jobs() ->
    case ergon_sql:query({system, job_metrics}, []) of
        {ok, #{rows := Rows}} ->
            maps:from_list([
                {Queue, #{
                    runnable => Runnable,
                    blocked => Blocked,
                    scheduled => Scheduled,
                    executing => Executing,
                    failed => Failed,
                    discarded => Discarded
                }}
             || {Queue, Runnable, Blocked, Scheduled, Executing, Failed, Discarded} <:- Rows
            ]);
        {error, _} ->
            #{}
    end.

%% Reported per queue rather than aborting the whole check, so one dropped queue
%% does not hide the state of the others.
pgmq_metrics(Queue) ->
    case ergon_pgmq:metrics(Queue) of
        {ok, Metrics} -> Metrics;
        {error, _} = Error -> Error
    end.
