-- Application-time time travel: every job version whose valid_period contained
-- instant $1, i.e. the truth about the world as of $1. Reads the bi-temporal
-- history maintained by FOR PORTION OF splits.
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
FROM ergon.jobs_asof($1);
