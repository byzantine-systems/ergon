-- pgmq_release_leases(queue): force-expire every in-flight visibility lease on
-- a queue. Class: on_change (ergon_migrate:sources/1).
--
-- plpgsql because the queue table name (pgmq.q_<queue>) cannot be
-- parameterised. Called by ergon_pgmq:release_leases/1 and ergon_reconciler.
-- Ergon owns no queues itself; this only installs the helper that operates on
-- whatever queues a host creates via ergon_migration:pgmq_queue/1.
--
-- The body resolves pgmq.q_%I dynamically at call time, so nothing here
-- depends on a queue (or on ergon.jobs) existing yet.
--
-- Explicitly `ergon.`, never bare: every object Ergon owns lives in the ergon
-- schema, and a bare name would land wherever the connecting role's
-- search_path happens to point.
CREATE OR REPLACE FUNCTION ergon.pgmq_release_leases (queue_name text)
    RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
    released bigint;
BEGIN
    -- statement_timestamp(), and the same one on both sides. It is STABLE, so
    -- the planner folds it once and can use it as a range bound on the vt index
    -- instead of re-evaluating per row, which is what a VOLATILE
    -- clock_timestamp() in a WHERE clause costs. It also makes the predicate and
    -- the assignment agree: two clock_timestamp() calls are two different
    -- instants, so a row could be selected as "still leased" and then written
    -- with a vt fractionally later than the one that chose it.
    EXECUTE FORMAT('UPDATE pgmq.q_%I SET vt = statement_timestamp() WHERE vt > statement_timestamp()', queue_name);
    GET DIAGNOSTICS released = ROW_COUNT;
    RETURN released;
END
$$;

