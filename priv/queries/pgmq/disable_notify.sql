-- Stop notifying for a pgmq queue. The queue itself is untouched; consumers
-- fall back to their poll interval, which is always the durable path anyway.
-- $1: queue name
DELETE FROM ergon.pgmq_notify_queues
WHERE queue_name = $1;

