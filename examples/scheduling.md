# Scheduling with `pg_cron`

Recurring SQL, guarded so the same code runs where pg_cron is not installed.

## The Helpers

```erlang
ok = ergon_cron:schedule(~"hourly-report", ~"0 * * * *", ~"SELECT hourly_report()"),
ok = ergon_cron:unschedule(~"hourly-report"),

{ok, Jobs} = ergon_cron:scheduled().
```

`Spec` is standard five-field cron syntax, or one of pg_cron's extensions such as `@weekly` or `1 second`.

For a migration, the SQL-only forms:

```erlang
ergon_cron:schedule_sql(~"hourly-report", ~"0 * * * *", ~"SELECT hourly_report()")
```

## Two properties worth relying on

**Guarded.** pg_cron can be `CREATE EXTENSION`'d in exactly one database per cluster, named by `cron.database_name`. So every statement these helpers emit is wrapped in an extension check and is a no-op where it is absent. That is what lets one migration run unchanged against a development database that has pg_cron and a test database that does not.

**Idempotent.** `cron.schedule` upserts by name, so scheduling the same job twice updates it in place rather than creating a duplicate. Exactly the contract a re-runnable migration needs, and why Ergon's own cron scripts are class `always` rather than `once`: a restored dump or a cloned database would otherwise leave the journal claiming a tick was installed while none exists.

## Why Ergon notifies with a cron tick and not a trigger

The obvious design for "wake a worker when a job arrives" is a trigger that calls `pg_notify` on insert. Ergon deliberately does not do that, and the reason is a property of PostgreSQL rather than a preference.

Every transaction with a pending `NOTIFY` takes the **global** notification-queue lock at commit, held until the transaction is fully committed and flushed. So notifying commits serialise against each other, cluster-wide. A trigger firing per insert therefore caps enqueue throughput no matter how much hardware is available.

So writers never call `pg_notify`. Instead a one-second tick does it for them:

```sql
-- priv/migrations/routines: one pg_notify per queue with runnable work,
-- from a single transaction
SELECT count(pg_notify('ergon_job_available', q.queue))::integer
FROM (SELECT DISTINCT queue FROM ergon.jobs
      WHERE state = 'available' AND scheduled_at <= now()
        AND upper(valid_period) = 'infinity') AS q
```

At most one notification per queue per second regardless of how many jobs arrive, so a batch enqueue of a thousand jobs produces a single wake. The tick is level-triggered on "runnable work exists" rather than edge-triggered on an insert, which means it keeps re-waking until a queue is drained and a future-scheduled retry wakes its workers on the first tick after it comes due.

The same applies to pgmq: **one** tick reads a registry and notifies every registered queue holding visible work, rather than one cron job per queue. The count of notifying transactions stays at one per second however many queues exist.

## One job per thing is sometimes fine

`ergon_migration:partition_lifecycle_sql/1` schedules one **weekly** job per partitioned table, and that is correct at any table count.

The difference is not the number of jobs, it is what they do. A per-second tick that calls `pg_notify` is a notifying transaction per queue per second, and every one takes that global lock. A weekly job that creates partitions takes no such lock and runs thousands of times less often.

## Latency floor

`pg_cron`'s finest granularity is one second, so any wake path built on it has a one-second floor. That is a deliberate trade: Ergon keeps no in-memory notification buffer to lose on a crash, and the tick recovers by itself from any missed notification.

If you need lower latency on a `pgmq` queue, use [long polling](pgmq.md#three-ways-to-wake-a-consumer), which blocks server-side and returns in ~100 ms. For jobs, lower the worker's `poll_interval`, the poll is the durable path either way.

## Escaping

The SQL you schedule is itself SQL and very often contains quoted literals:

```erlang
ergon_cron:schedule(~"cleanup", ~"@daily", ~"DELETE FROM audit WHERE kind = 'noise'").
```

`ergon_cron` doubles internal single quotes when it builds the `cron.schedule` call, so that works. Note these helpers take developer-authored SQL, not user input, and are not a sanitiser: do not build a schedule command out of anything that came from a request.
