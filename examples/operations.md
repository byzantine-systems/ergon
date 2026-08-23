# Operations

Health checks, recovery, derived-state drift, and boot-time partition safety.

## Health

```erlang
#{db := ok,
  extensions := #{~"pgmq" := ~"1.12.0", ~"pg_cron" := ~"1.6", ...},
  jobs := #{~"mailers" := #{runnable := 12, blocked := 4, scheduled := 2,
                            executing := 3, failed := 1, discarded := 0}},
  pgmq := #{~"events" := #{queue_length := 5, queue_visible_length := 3,
                           oldest_msg_age_sec := 12}}} = ergon_health:check().
```

Never raises. Every section reports its own failure, because a health check that crashes when the database is down is answering the wrong question.

`pgmq` queues have to be named, since `pgmq` has no notion of which queues belong to your application and reporting on every queue in the database would include other applications:

```erlang
{ergon, [{ergon_health, [{pgmq_queues, [~"events", ~"receipts"]}]}]}
```

Job queues need no such list. They are rows in `ergon.jobs`, so the query finds them.

### Reading the job counts

`runnable` is what checkout would take next, using the same predicate as the fetch index.

**`blocked` is the one to watch.** Those jobs are available but waiting on a workflow parent, so they are invisible to checkout, and a queue full of them looks idle while holding work. Since a parent that ended `failed` or `discarded` never completes, its children stay blocked until someone intervenes. Nothing else surfaces that. Clear it with `ergon:cancel/1` on the stuck root, or fix and re-run the parent.

A persistently high `executing` with no throughput means consumers died holding jobs, which is what the reconciler is for.

Also, without `pg_cron`, no notification tick runs and every wake path silently falls back to polling.

## Recovery

Run the reconciler after a node dies mid-flight, after a failover, or on any restart where something may have been holding work when it stopped.

```erlang
Summary = ergon_reconciler:run(#{
    pgmq_queues => [~"events"],
    hydrate => fun my_app_state:stop_all_and_rebuild/0
}).
```

Three moves, and the order matters:

1. **Your `hydrate` callback**, first. Ergon does not know what in-memory state you keep, so this is where you stop suspect processes and rebuild them. It runs before anything is released because releasing messages first would hand redelivered work to processes that are about to be killed.

2. **Release stranded `pgmq` leases.** A consumer that died mid-processing left its messages invisible until their visibility timeout expires, which may be thirty seconds or thirty minutes. This expires them all at once, then snapshots metrics so they describe the recovered queue rather than the broken one.

3. **Check `pending_parents` for drift.**

A host with no in-memory state can omit `hydrate` entirely.

## Derived-state drift

`ergon.jobs.pending_parents` counts a job's incomplete workflow parents, and it
lives inside the fetch index's predicate so blocked jobs are absent from the
index rather than scanned past. That denormalisation is what keeps checkout
proportional to the batch size instead of to the blocked backlog, and it is also
what makes the counter unverifiable by ordinary means: nothing reads `job_edges`
on the hot path any more.

So if the maintaining triggers have a bug, or someone writes the column by hand,
a job ends up either permanently unrunnable (count too high) or running before
its parents finish (count too low). Neither surfaces anywhere.

```erlang
%% report only
[#{id := 42, actual := 2, expected := 0}] = ergon_reconciler:drift(),

%% fix it, deliberately
[42] = ergon_reconciler:repair_drift().
```

- Repair is **off by default**, including inside `run/1` unless you pass `repair => true`. Silently rewriting derived state hides whatever caused it to drift, and the drift is the more useful signal. Repair writes only the rows that are actually wrong, which matters because each one fires the versioning trigger and accrues a history row.
- `drift/0` is cheap enough to run on a schedule; the query returns only rows that disagree.

It also doubles as a cross-check on the workflow graph:
- `ergon:ready_children/0` answers the same question through an entirely independent mechanism, a graph match rather than a trigger-maintained counter, so persistent disagreement between them means one of the two is wrong.

## Partition safety at boot

```erlang
%% in your supervision tree, ahead of anything that writes to the table
#{id => partitions,
  start => {ergon_partition_boot_check, start_link,
            [#{table => ~"telemetry", months_ahead => 2}]}}
```

The weekly cron job is what normally keeps the partition horizon ahead of ingestion. This closes the gap when it has not: a silently dead cron daemon, a clock-skewed failover, or a fresh restore of an old backup all leave ingestion facing a table that cannot accept inserts.

- **It blocks your supervisor on purpose.** The check runs in `init/1`, so start-up waits for it, because children later in the tree must not come up against a table that cannot accept inserts. Failing to boot is a better outcome than booting into guaranteed write errors.
- For the same reason, if partitions are still missing after remediation it **raises** rather than logging. At that point something is wrong that a retry will not fix, most likely the manage function not existing at all.
- Once the check passes the process hibernates, it has nothing left to do.

Disable it globally in tests, where the application boots before any fixture table exists, and start supervised instances explicitly:

```erlang
{ergon, [{ergon_partition_boot_check, [{enabled, false}]}]}
```

## What to alert on

| Signal | Meaning |
|---|---|
| `db` not `ok` | the pool cannot reach PostgreSQL |
| `pg_cron` missing from `extensions` | no notification tick, everything falls back to polling |
| `blocked` rising and not falling | a workflow is stuck behind a parent that will never complete |
| `executing` high with no throughput | consumers died holding jobs, run the reconciler |
| `queue_visible_length` far below `queue_length` | messages stranded behind visibility leases |
| `drift/0` non-empty | `pending_parents` disagrees with the edges, investigate before repairing |
