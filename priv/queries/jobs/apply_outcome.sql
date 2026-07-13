-- Persist a state transition for a single job.
--
-- Uses PostgreSQL 19's UPDATE ... FOR PORTION OF so the job's currently-open
-- validity window is closed at now() and a fresh row is written for the new
-- state. History is preserved for auditing rather than being overwritten in
-- place. Only the live row (upper(valid_period) = 'infinity') is affected.
--
-- On a retry (new state 'available') scheduled_at is pushed out by a capped
-- quadratic backoff computed from the attempt count ($3): 1s, 4s, 9s, ... up to
-- 100s. Other transitions leave scheduled_at untouched. The legality of the
-- transition itself is enforced by the jobs_transition_guard trigger.
UPDATE ergon.jobs
FOR PORTION OF valid_period FROM now() TO 'infinity'
SET state = $2::ergon.job_state,
attempt = $3,
last_error = $4,
scheduled_at = CASE
WHEN $2 = 'available'
THEN now() + make_interval(secs => power(least($3, 10), 2))
ELSE scheduled_at
END
WHERE id = $1
AND upper(valid_period) = 'infinity'
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
