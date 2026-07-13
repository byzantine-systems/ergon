-- Atomically check out up to $2 available jobs from queue $1, marking them
-- 'executing' and consuming an attempt.
--
-- Uses the classic FOR UPDATE SKIP LOCKED pattern so concurrent workers never
-- contend for the same job. Only rows whose validity window is still open
-- (upper(valid_period) = 'infinity') are considered live.
UPDATE ergon.jobs AS job
SET state = 'executing', attempt = attempt + 1
WHERE
    job.id IN (
        SELECT candidate.id
        FROM ergon.jobs AS candidate
        WHERE
            candidate.queue = $1
            AND candidate.state = 'available'
            AND candidate.scheduled_at <= now()
            AND upper(candidate.valid_period) = 'infinity'
        ORDER BY candidate.scheduled_at
        FOR UPDATE SKIP LOCKED
        LIMIT $2
    )
    AND upper(job.valid_period) = 'infinity'
RETURNING
    job.id,
    job.queue,
    job.worker,
    job.payload::text AS payload,
    job.state,
    job.fingerprint,
    job.attempt,
    job.max_attempts,
    job.last_error,
    job.scheduled_at,
    job.inserted_at;
