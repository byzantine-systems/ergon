# Scheduling with pg_cron

Ergon schedules recurring SQL with [pg_cron](https://github.com/citusdata/pg_cron)
rather than an application-side scheduler. `import Ergon.Cron` in a migration for
guarded `schedule/3` and `unschedule/1` helpers.

## Scheduling and unscheduling

`schedule/3` takes a name, a cron spec, and the SQL to run. Call it from a
migration's `up/0`, and reverse it in `down/0`:

```elixir
defmodule MyApp.Repo.Migrations.ScheduleReports do
  use Ecto.Migration
  import Ergon.Cron

  def up do
    schedule("hourly-report", "0 * * * *", "SELECT hourly_report()")
  end

  def down do
    unschedule("hourly-report")
  end
end
```

The spec is standard 5-field Vixie-cron. pg_cron 1.6+ also accepts interval
syntax such as `"1 second"` or `"@weekly"`, which Ergon's own helpers use for the
notify ticks and partition lifecycle.

## Idempotent by name

`schedule/3` is idempotent via `cron.schedule`'s upsert-by-name semantics:
calling it twice with the same name updates the schedule/command in place rather
than creating a duplicate, exactly the contract a re-runnable migration needs.

## Guarded: dev has pg_cron, test doesn't

pg_cron can be `CREATE EXTENSION`'d in exactly one database per cluster (set by
`cron.database_name` in `postgresql.conf`). The dev database has it, the test
database does not, and host clusters vary. Every helper here, and every
pg_cron-dependent helper in [`Ergon.Migration`](migrations.md), is a **guarded
no-op** when pg_cron is absent:

```sql
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    PERFORM cron.schedule(...);
  END IF;
END
$$
```

So the same migration runs cleanly in dev (cron active) and test (cron absent),
and the features that lean on cron degrade to their fallback behaviour rather than
failing.

## Why Ergon prefers cron ticks to triggers

Several of Ergon's own mechanisms, the pgmq notify path
([`pgmq_notify_cron/2`](migrations.md#pgmq-queues)), the `ergon.jobs` wake-up
([`job_notify_cron/0`](operations.md#the-job-notifier)), and partition lifecycle
are driven by a per-second (or weekly) pg_cron tick rather than a row trigger.

The reason is the same in each case: a transaction with a pending `NOTIFY` takes
the **global notification-queue lock at commit**, so a trigger that notifies
would serialize every enqueuing commit, the LISTEN/NOTIFY scalability trap. With
a cron tick:

- Writers never call `pg_notify`; only the tick's own transaction does.
- The tick is **level-triggered**, it re-notifies as long as deliverable work
  exists, so a lost wake self-heals within a second.
- Future-scheduled work (retries, backoffs) is notified exactly when it comes
  due (`scheduled_at <= now()`), something an insert trigger could never do.

When pg_cron is absent the tick never runs, no `NOTIFY` ever fires, and consumers
fall back to their poll loop, still fully correct, only slower.
