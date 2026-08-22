-- Jobs whose pending_parents disagrees with what ergon.job_edges says it should
-- be. Empty when healthy.
--
-- pending_parents is derived state: maintained incrementally by the
-- ergon.block_child and ergon.unblock_children triggers, and denormalised onto
-- the row so jobs_fetch_idx can carry it in its predicate. That denormalisation
-- is what keeps checkout O(LIMIT) instead of scanning past every blocked job,
-- and it is also why nothing else can verify the counter. A trigger bug or a
-- manual UPDATE leaves a job either permanently unrunnable (count too high) or
-- running before its parents finish (count too low), with nothing to notice.
--
-- This recomputes the truth from scratch and reports only the rows that
-- disagree, so it is cheap to run often and its output is actionable rather than
-- a full dump.
--
-- Counts only LIVE parents that have not completed, matching ergon.block_child
-- exactly. An edge whose parent row no longer exists contributes nothing, which
-- is the same answer the triggers give: there is no foreign key from
-- ergon.job_edges to ergon.jobs, so a dangling edge is possible and is treated
-- as no longer blocking.
SELECT
    child.id,
    child.pending_parents AS actual,
    COALESCE(expected.expected, 0)::int AS expected
FROM
    ergon.jobs_current AS child
    LEFT JOIN (
        SELECT
            edge.child_id,
            COUNT(*)::int AS expected
        FROM
            ergon.job_edges AS edge
            JOIN ergon.jobs_current AS parent ON parent.id = edge.parent_id
        WHERE
            parent.state <> 'completed'
        GROUP BY
            edge.child_id) AS expected ON expected.child_id = child.id
WHERE
    child.pending_parents IS DISTINCT FROM COALESCE(expected.expected, 0)
ORDER BY
    child.id;

