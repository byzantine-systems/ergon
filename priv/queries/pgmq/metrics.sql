-- Health snapshot of one pgmq queue. queue_length counts every message in
-- the queue table. queue_visible_length only those deliverable right now
-- the difference is messages hidden behind a visibility lease (in-flight
-- or stranded by a dead consumer). Used by the reconciler and Ergon.Health.
-- $1: queue name
SELECT
    queue_length,
    queue_visible_length,
    oldest_msg_age_sec
FROM pgmq.metrics($1);
