-- Wake-up tick body for ergon_job_notifier. Class: on_change
-- (ergon_migrate:sources/1). Applies after schema/: the SQL body reads
-- ergon.jobs, and a LANGUAGE sql body is validated at CREATE time.
--
-- Writers never call pg_notify. Every transaction with a pending NOTIFY takes
-- the global notification-queue lock at commit, which serializes every
-- enqueuing commit. Instead this notifies once per queue that has immediately
-- runnable work, and pg_cron runs it every second
-- (../cron/000014_job_notify_cron.sql). The WHERE clause is deliberately the
-- exact predicate of jobs_fetch_idx, including pending_parents = 0.
--
-- That last conjunct is not decoration. Without it a queue holding nothing but
-- children blocked behind unfinished parents notifies every second, waking every
-- worker on that queue to issue a checkout that cannot match anything, because
-- the same predicate keeps those rows out of the index the checkout reads. A
-- workflow-heavy queue would spend the whole time waking up to find nothing,
-- which is precisely the cost this tick exists to avoid.
--
-- The payload is the queue name only, never job data or tenant: NOTIFY
-- bypasses RLS, so anything richer would leak across tenants. The channel is
-- fixed and mirrored by ergon_job_notifier:channel/0.
CREATE OR REPLACE FUNCTION ergon.notify_pending_jobs ()
    RETURNS integer
    LANGUAGE sql
    AS $$
    SELECT
        COUNT(PG_NOTIFY('ergon_job_available', q.queue))::integer
    FROM ( SELECT DISTINCT
            queue
        FROM
            ergon.jobs
        WHERE
            state = 'available'
            AND scheduled_at <= NOW()
            AND UPPER(valid_period) = 'infinity'
            AND pending_parents = 0) AS q
$$;

