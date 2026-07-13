-- Insert a new job (get-or-create) and return the full materialised row.
--
-- Delegates to ergon.enqueue, which inserts the job and, on the temporal
-- uniqueness EXCLUDE conflict, returns the existing live overlapping job
-- instead of raising, so enqueuing a duplicate unique job is a no-op that
-- yields the incumbent. The fingerprint is generated in-DB now, not 
-- passed in.
-- $1: queue
-- $2: worker
-- $3: payload (jsonb)
-- $4: max_attempts
-- (0 = non-unique / empty window, >0 = unique-for-N)
-- $5: dedup window in seconds
SELECT
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
    inserted_at
FROM ergon.enqueue($1, $2, $3::jsonb, $4, $5);
