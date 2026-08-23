-- The 1 s notifier tick. Class: always (ergon_migrate:sources/1).
--
-- `always` rather than `once`: cron.job rows live in the pg_cron database and
-- are not part of the ergon schema, so a restored dump, a cloned database, or
-- a manually unscheduled job would leave the journal claiming this was applied
-- while no tick exists. Re-asserting it on every run is cheap and idempotent,
-- cron.schedule upserts by name (pg_cron 1.6+, pinned in flake.nix), so this
-- updates the schedule in place rather than creating duplicates.
--
-- Guarded no-op where pg_cron is absent (the test database, cron-less host
-- clusters). There the workers' fallback poll is the only wake path, which is
-- fully correct, only slower.
DO $$
BEGIN
    IF EXISTS (
        SELECT
            1
        FROM
            pg_extension
        WHERE
            extname = 'pg_cron') THEN
    PERFORM
        cron.schedule ('ergon-job-notify', '1 second', 'SELECT ergon.notify_pending_jobs()');
END IF;
END
$$;

