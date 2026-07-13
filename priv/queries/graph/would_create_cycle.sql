-- Whether adding the edge parent=$1 -> child=$2 would introduce a cycle, i.e.
-- whether $1 is already reachable from $2 (the new edge would then close a
-- loop). Also true for a self-loop ($1 = $2), since $2 is included in its own
-- reachable set. Used by Ergon.DB.link/3 to reject cyclic dependencies before
-- inserting the edge.
-- $1: proposed parent id
-- $2: proposed child id
WITH RECURSIVE
    reachable (id) AS (
        SELECT $2::bigint
        UNION
        SELECT edges.child_id
        FROM ergon.job_edges AS edges
            JOIN reachable ON edges.parent_id = reachable.id
    )

SELECT EXISTS(
    SELECT 1 FROM reachable
    WHERE id = $1
) AS would_cycle;
