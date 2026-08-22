-- Create a pgmq queue and its archive table.
--
-- Idempotent: calling it for a queue that already exists succeeds and changes
-- nothing, so setup code is safely re-runnable. The name is bound rather than
-- interpolated: pgmq.create takes text and derives pgmq.q_<name> itself.
--
-- The IS NOT NULL is a cast, not a test. pgmq.create returns void, and no
-- pg_types codec claims void's typsend, so projecting the bare call hands the
-- driver an unknown OID carrying an empty binary and its decoder raises a
-- function_clause. Comparing forces a boolean column, which decodes normally.
-- The value is always true; only the side effect matters.
-- $1: queue name
SELECT
    pgmq.create ($1) IS NOT NULL AS created;

