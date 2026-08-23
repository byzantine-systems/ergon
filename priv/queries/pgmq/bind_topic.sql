-- Bind a routing pattern to a queue, so a matching send_topic reaches it.
-- Idempotent: binding the same pattern to the same queue again is a no-op.
--
-- The IS NOT NULL is a cast, not a test, for the same reason as in
-- create_queue.sql: pgmq.bind_topic returns void, which no pg_types codec can
-- decode.
-- $1: pattern
-- $2: queue name
SELECT
    pgmq.bind_topic ($1, $2) IS NOT NULL AS bound;

