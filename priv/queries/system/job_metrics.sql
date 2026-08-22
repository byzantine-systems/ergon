-- Per-queue job counts, for the health endpoint.
--
-- Reads ergon.jobs_current, so every count is of live rows only and superseded
-- valid-time versions are excluded. The buckets are chosen to answer "is this
-- queue healthy" rather than to tally states:
--
--   runnable   available, due, and not blocked. Exactly what checkout would
--              take next, and deliberately the same predicate as
--              jobs_fetch_idx so the two cannot drift apart.
--   blocked    available but waiting on a workflow parent. Counted separately
--              because these are invisible to checkout and a queue full of
--              them looks idle while holding work that may never run. A parent
--              that ended failed or discarded never completes, so its children
--              stay here until an operator intervenes.
--   scheduled  available but not yet due, i.e. a retry serving out its backoff.
--   executing  checked out and in flight. A persistently high count with no
--              throughput means consumers died holding jobs, which is what the
--              reconciler is for.
--   failed     attempts exhausted.
--   discarded  cancelled.
SELECT
    queue,
    COUNT(*) FILTER (WHERE state = 'available'
        AND pending_parents = 0
        AND scheduled_at <= NOW())::int AS runnable,
    COUNT(*) FILTER (WHERE state = 'available'
        AND pending_parents > 0)::int AS blocked,
    COUNT(*) FILTER (WHERE state = 'available'
        AND pending_parents = 0
        AND scheduled_at > NOW())::int AS scheduled,
    COUNT(*) FILTER (WHERE state = 'executing')::int AS executing,
    COUNT(*) FILTER (WHERE state = 'failed')::int AS failed,
    COUNT(*) FILTER (WHERE state = 'discarded')::int AS discarded
FROM
    ergon.jobs_current
GROUP BY
    queue
ORDER BY
    queue;

