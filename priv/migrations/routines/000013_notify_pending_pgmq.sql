-- Wake-up tick body for ergon_pgmq_consumer. Class: on_change
-- (ergon_migrate:sources/1). Applies after schema/, because it reads
-- ergon.pgmq_notify_queues.
--
-- The pgmq counterpart of ergon.notify_pending_jobs(), and deliberately the
-- same shape: ONE function, called by ONE cron job, emitting one pg_notify per
-- queue that has deliverable work. Writers never call pg_notify. Every
-- transaction with a pending NOTIFY takes the global notification-queue lock at
-- commit, so what costs throughput is the number of notifying transactions, not
-- the number of notifications. Installing a cron job per queue would make that
-- number grow with the queue count; this keeps it at one per second regardless.
--
-- Only registered queues are considered, so Ergon never notifies on a channel
-- belonging to another application that happens to share this database and its
-- pgmq extension.
--
-- plpgsql rather than LANGUAGE sql because the queue table name varies per row,
-- so the visibility probe has to be dynamic. format('%I') quotes the identifier,
-- which is what keeps a queue name from reaching the parser as raw text.
--
-- The payload is empty by design. A listener is expected to read pgmq itself
-- rather than trust the notification, matching pgmq's own at-least-once
-- contract: a dropped NOTIFY costs latency, never a message.
--
-- vt <= statement_timestamp() rather than now(): now() is frozen for the whole
-- transaction, so a message whose visibility timeout expires while the tick runs
-- would be missed until the next one. statement_timestamp() advances per
-- statement, and each queue's probe below is its own statement, so the tick sees
-- an instant that moves as it works through the registered queues.
--
-- Not clock_timestamp(), which is VOLATILE. In a WHERE clause that is expensive
-- rather than merely untidy: the planner cannot fold a volatile call to a
-- constant, so it loses `vt` as an index range bound and re-evaluates per row.
-- statement_timestamp() is STABLE and plans as a parameter.
CREATE OR REPLACE FUNCTION ergon.notify_pending_pgmq ()
    RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    registered record;
    has_visible boolean;
    notified integer := 0;
BEGIN
    FOR registered IN
    SELECT
        queue_name,
        channel
    FROM
        ergon.pgmq_notify_queues LOOP
            -- A registered queue whose pgmq table has been dropped should not
            -- take the whole tick down with it.
            BEGIN
                EXECUTE FORMAT('SELECT EXISTS (SELECT 1 FROM pgmq.q_%I WHERE vt <= statement_timestamp())', registered.queue_name)
INTO
    has_visible;
            EXCEPTION
                WHEN undefined_table THEN
                    CONTINUE;
            END;
            IF has_visible THEN
                PERFORM
                    PG_NOTIFY(registered.channel, '');
                    notified := notified + 1;
                END IF;
    END LOOP;
    RETURN notified;
END
$$;

