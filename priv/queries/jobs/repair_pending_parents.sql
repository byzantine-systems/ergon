-- Rewrite pending_parents from ergon.job_edges for the jobs that disagree.
-- Returns the ids actually corrected.
--
-- The repair half of pending_parents_drift.sql, and deliberately opt-in:
-- ergon_reconciler reports drift by default and only writes when asked. Silently
-- rewriting derived state hides whatever caused it to drift, which is the
-- opposite of what a reconciler is for.
--
-- Touches only rows that are actually wrong, so it writes nothing on a healthy
-- database. That matters more than it looks: every row this updates fires the
-- versioning trigger and accrues a history row.
UPDATE
    ergon.jobs AS child
SET
    pending_parents = truth.expected
FROM (
    SELECT
        live.id,
        COALESCE((
            SELECT
                COUNT(*)::int
            FROM ergon.job_edges AS edge
            JOIN ergon.jobs_current AS parent ON parent.id = edge.parent_id
            WHERE
                edge.child_id = live.id
                AND parent.state <> 'completed'), 0) AS expected
    FROM
        ergon.jobs_current AS live) AS truth
WHERE
    child.id = truth.id
    AND UPPER(child.valid_period) = 'infinity'
AND child.pending_parents IS DISTINCT FROM truth.expected
RETURNING
    child.id;

