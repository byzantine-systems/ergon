-- Drop a pgmq queue and its archive table.
-- $1: queue name
SELECT
    pgmq.drop_queue ($1);

