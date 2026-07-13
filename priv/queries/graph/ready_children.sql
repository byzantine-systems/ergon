-- Every 'available' job whose workflow parents have ALL completed, and which
-- may therefore now be checked out.
--
-- The MATCH walks every 'triggers' edge into an available child, projecting
-- each parent's state, grouping by child and keeping only those where
-- bool_and(parent completed) holds yields exactly the ready children. This is
-- a single SQL/PGQ round-trip in place of a recursive CTE.
SELECT edges.child_id
FROM
    GRAPH_TABLE(
        ergon.workflow
        match (parent IS job)
        - [IS triggers]
        -> (child IS job WHERE child.state = 'available')
        columns (child.id AS child_id, parent.state AS parent_state)
    ) AS edges
GROUP BY edges.child_id
HAVING BOOL_AND(edges.parent_state = 'completed');
