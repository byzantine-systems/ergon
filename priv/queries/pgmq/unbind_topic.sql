-- Remove a pattern binding. Returns whether a binding was actually removed.
-- $1: pattern
-- $2: queue name
SELECT
    pgmq.unbind_topic ($1, $2) AS unbound;

