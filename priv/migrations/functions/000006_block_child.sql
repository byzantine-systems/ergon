-- Maintain ergon.jobs.pending_parents as edges are added and removed.
-- Class: on_change (ergon_migrate:sources/1). Applies before schema/, because
-- job_edges_block_child_trigger there attaches it.
--
-- pending_parents is the count of a job's parents that have not yet completed,
-- and it exists so that "is this job blocked?" is answerable from the row itself
-- rather than from a join. That is what lets jobs_fetch_idx carry
-- `pending_parents = 0` in its predicate, which keeps a blocked job out of the
-- index entirely instead of making checkout scan past it. See the index comment
-- in ../schema for the measurements.
--
-- This is incremental view maintenance done by hand: the counter is a count(*)
-- over incomplete parents, updated by deltas rather than recomputed. The reason
-- it is hand-rolled rather than an incrementally maintained materialized view is
-- index locality. A materialized view can only ever land in its own relation,
-- which leaves the predicate outside jobs_fetch_idx and the scan depth unchanged
-- (measured: 42 ms against 0.14 ms, because both still walk 50k blocked rows).
-- Denormalising onto the row is the whole point.
--
-- Adding an edge to an ALREADY completed parent adds no block, so the counter is
-- only incremented when the parent is live and unfinished. Deletion mirrors that
-- exactly, which is what keeps the count non-negative without clamping: a given
-- edge contributes +1 then -1, or 0 then 0, and a parent that reached 'completed'
-- is terminal so it can never re-block anyone.
--
-- The counter is derived state and derived state can drift. The CHECK on the
-- column is deliberately not clamped, so a genuine bug surfaces as a constraint
-- violation rather than as a job that is silently unrunnable forever.
CREATE OR REPLACE FUNCTION ergon.block_child ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge record;
    delta integer;
BEGIN
    IF tg_op = 'DELETE' THEN
        edge := old;
        delta := - 1;
    ELSE
        edge := new;
        delta := 1;
    END IF;
    -- Only a live, not-yet-completed parent blocks anything.
    IF EXISTS (
        SELECT
            1
        FROM
            ergon.jobs AS parent
        WHERE
            parent.id = edge.parent_id
            AND UPPER(parent.valid_period) = 'infinity'
            AND parent.state <> 'completed') THEN
    UPDATE
        ergon.jobs
    SET
        pending_parents = pending_parents + delta
    WHERE
        id = edge.child_id
        AND UPPER(valid_period) = 'infinity';
END IF;
    RETURN NULL;
END
$$;

