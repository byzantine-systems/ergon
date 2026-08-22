-- Decrement pending_parents on every child when a parent reaches 'completed'.
-- Class: on_change (ergon_migrate:sources/1). Applies before schema/, because
-- jobs_unblock_children_trigger there attaches it.
--
-- The other half of the counter maintained by ergon.block_child(). Together they
-- are what makes a blocked job absent from jobs_fetch_idx rather than merely
-- filtered out of it.
--
-- Fires exactly once per completion, which the WHEN clause on the trigger is
-- responsible for and which is subtler than it looks. apply_outcome.sql
-- transitions state with UPDATE ... FOR PORTION OF, so a completion both UPDATEs
-- the matched row (to state 'completed', valid_period [now, infinity)) and writes
-- the superseded portion as a separate row ending at now(). Requiring
-- upper(NEW.valid_period) = 'infinity' keeps this to the live row, and requiring
-- the state to actually change keeps a no-op update from double-decrementing.
--
-- No recursion: the UPDATE below changes only pending_parents, leaving state
-- untouched, so the trigger's own WHEN clause excludes it. It does still fire
-- jobs_versioning_trigger, so each child accrues one history row per parent
-- completion. That is accepted cost, and arguably correct: the child's
-- runnability genuinely changed and the audit log should say so.
--
-- Note that only 'completed' unblocks. A parent that ends 'failed' or
-- 'discarded' leaves its children blocked indefinitely, which is the honest
-- reading of a dependency: the workflow is stuck and wants an operator, not a
-- child that runs as though its prerequisite had succeeded. cancel_cascade
-- discards descendants outright rather than stranding them.
CREATE OR REPLACE FUNCTION ergon.unblock_children ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE
        ergon.jobs AS child
    SET
        pending_parents = child.pending_parents - 1
    WHERE
        UPPER(child.valid_period) = 'infinity'
        AND child.id IN (
            SELECT
                edge.child_id
            FROM
                ergon.job_edges AS edge
            WHERE
                edge.parent_id = NEW.id);
    RETURN NULL;
END
$$;

