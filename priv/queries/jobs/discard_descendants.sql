-- Cascade-cancel: discard $1 and every descendant still cancellable (available
-- or executing), each via FOR PORTION OF so the discard is a proper valid-time
-- transition (history preserved, jobs_transition_guard satisfied, both
-- available->discarded and executing->discarded are legal). Terminal
-- descendants (completed/failed/already discarded) are left untouched. Returns
-- the full row of each job actually discarded.
-- $1: root job id
UPDATE ergon.jobs
FOR PORTION OF valid_period FROM now() TO 'infinity'
SET state = 'discarded'
WHERE upper(valid_period) = 'infinity'
AND state IN ('available', 'executing')
AND id IN (
WITH RECURSIVE tree(id) AS (
SELECT $1::bigint
UNION
SELECT edges.child_id
FROM ergon.job_edges AS edges
JOIN tree ON edges.parent_id = tree.id
)
SELECT id FROM tree
)
RETURNING
id,
queue,
worker,
payload::text AS payload,
state,
fingerprint,
attempt,
max_attempts,
last_error,
scheduled_at,
inserted_at;
