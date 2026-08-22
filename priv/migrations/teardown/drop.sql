-- Reverses the whole ergon migration set. Executed by ergon_migrate:teardown/1.
--
-- NOT a migraterl source: this directory is deliberately absent from
-- ergon_migrate:sources/1, because migraterl is forward-only and would
-- otherwise apply this as just another script. teardown/1 runs it directly and
-- then clears the `ergon` namespace from migraterl.schema_journal, so a
-- subsequent migrate/1 replays from scratch.
--
-- Order matters twice over:
--   1. The cron ticks go first. cron.job rows live in the pg_cron catalog,
--      not in the ergon schema, so the CASCADE below does not touch them and
--      an orphaned tick would keep calling a function that no longer exists.
--   2. Property graphs are not dropped by DROP SCHEMA ... CASCADE, so
--      ergon.workflow must be dropped explicitly before the schema goes.
--
-- Everything else is removed by the cascade, because every object Ergon owns
-- lives in the ergon schema: the job_state domain, jobs + jobs_history, the RLS
-- policy, triggers, indexes, the jobs_current view, and all seven routines
-- (enqueue, enforce_job_transition, notify_pending_jobs, jobs_asof,
-- jobs_asof_system, temporal_versioning, pgmq_release_leases). Nothing needs a
-- separate DROP FUNCTION.
--
-- Extension drops (btree_gist, pgcrypto, pgmq, pg_cron) are intentionally not
-- mirrored: they may serve other databases or applications, and are cheap to
-- leave installed.
DO $$
DECLARE
    tick text;
BEGIN
    IF EXISTS (
        SELECT
            1
        FROM
            pg_extension
        WHERE
            extname = 'pg_cron') THEN
    FOREACH tick IN ARRAY ARRAY['ergon-job-notify', 'ergon-pgmq-notify'] LOOP
        IF EXISTS (
            SELECT
                1
            FROM
                cron.job
            WHERE
                jobname = tick) THEN
        PERFORM
            cron.unschedule (tick);
    END IF;
END LOOP;
END IF;
END
$$;

DROP PROPERTY GRAPH IF EXISTS ergon.workflow;

DROP SCHEMA IF EXISTS ergon CASCADE;

