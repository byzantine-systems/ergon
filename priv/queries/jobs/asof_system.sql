-- System-time time travel: what the database believed about each job as of
-- instant $1, spanning live rows and the archived ergon.jobs_history twin.
-- $1: instant (timestamptz)
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
FROM ergon.jobs_asof_system($1);
