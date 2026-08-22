-- The 1 s pgmq notifier tick. Class: always (ergon_migrate:sources/1).
--
-- `always` for the same reason as the job tick: cron.job rows live in the
-- pg_cron catalog rather than in the ergon schema, so a restored dump, a cloned
-- database, or a manually unscheduled job would leave the journal claiming this
-- was applied while no tick exists. Re-asserting it every run is cheap and
-- idempotent, since cron.schedule upserts by name (pg_cron 1.6+, pinned in
-- flake.nix).
--
-- Installed unconditionally, including for hosts that never touch pgmq. With an
-- empty ergon.pgmq_notify_queues that is one scan of an empty table per second,
-- which costs nothing measurable, and it keeps queue registration
-- (ergon_pgmq:enable_notify/1) a plain INSERT rather than a migration.
--
-- Guarded no-op where pg_cron is absent (the test database, cron-less host
-- clusters). There a consumer's poll_interval is the only wake path, which is
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
        cron.schedule ('ergon-pgmq-notify', '1 second', 'SELECT ergon.notify_pending_pgmq()');
END IF;
END
$$;

