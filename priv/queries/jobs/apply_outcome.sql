-- Persist a state transition for a single job.
--
-- Uses PostgreSQL 19's UPDATE ... FOR PORTION OF so the job's currently-open
-- validity window is closed at now() and a fresh row is written for the new
-- state. History is preserved for auditing rather than being overwritten in
-- place. Only the live row (upper(valid_period) = 'infinity') is affected.
--
-- On a retry (new state 'available') scheduled_at is pushed out by
-- ergon.retry_backoff, a jittered capped exponential taking the attempt count
-- ($3) and its tuning from ergon_db: base milliseconds ($5), cap milliseconds
-- ($6), and strategy ($7). See that function for the formulas and where they
-- come from. Other transitions leave scheduled_at untouched.
--
-- now() stays the anchor, so the timestamp is still the database's and still
-- transaction-frozen; only the offset carries the jitter. The legality of the
-- transition itself is enforced by the jobs_transition_guard trigger.
UPDATE
    ergon.jobs FOR PORTION OF valid_period
FROM
    NOW()
    TO 'infinity'
SET
    state = $2::ergon.job_state,
    attempt = $3,
    last_error = $4,
    scheduled_at = CASE WHEN $2 = 'available' THEN
        NOW() + ergon.retry_backoff ($3, $5, $6, $7)
    ELSE
        scheduled_at
    END
WHERE
    id = $1
    AND UPPER(valid_period) = 'infinity'
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

