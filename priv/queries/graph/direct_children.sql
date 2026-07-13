-- The direct children a completed job unblocks: every 'available' job reachable
-- in one 'triggers' hop from the given parent id ($1).
SELECT reached.child_id
FROM GRAPH_TABLE(
    ergon.workflow
    match (parent IS job WHERE parent.id = $1)
    - [IS triggers] -> (child IS job WHERE child.state = 'available')
    COLUMNS(child.id AS child_id)
) AS reached;
