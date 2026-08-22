-- Register a pgmq queue for wake-up notifications.
--
-- ergon.notify_pending_pgmq() -- one tick for every queue, see
-- ../../migrations/routines -- reads this table each second and notifies the
-- registered channel whenever the queue holds a visible message. Registration is
-- therefore a plain INSERT rather than a migration: no DDL, no cron job per
-- queue, and nothing to keep in step.
--
-- Upsert so re-registering with a different channel moves it rather than
-- failing, which is what makes host setup code re-runnable.
-- $1: queue name
-- $2: notification channel
INSERT INTO ergon.pgmq_notify_queues (queue_name, channel)
    VALUES ($1, $2)
ON CONFLICT (queue_name)
    DO UPDATE SET
        channel = excluded.channel;

