-- Atomically check out up to $2 available jobs from queue $1, marking them
-- 'executing' and consuming an attempt.
--
-- Uses the classic FOR UPDATE SKIP LOCKED pattern so concurrent workers never
-- contend for the same job. Only rows whose validity window is still open
-- (upper(valid_period) = 'infinity') are considered live.
--
-- Workflow dependencies are ENFORCED here, not merely advisory: pending_parents
-- counts the job's parents that have not yet completed, so `= 0` withholds a
-- blocked child. Without it a child was checkoutable the moment it was enqueued,
-- and ergon.workflow's ready_children was an informational query nothing
-- consulted.
--
-- The predicate is written as a plain column test, matching jobs_fetch_idx's
-- partial predicate exactly, so blocked jobs are absent from the index rather
-- than filtered out of it. Expressing the same rule as a join against
-- ergon.job_edges instead costs 237 ms where this costs 0.14 ms, because LIMIT
-- bounds the output but not the scan and the walk has to step over every blocked
-- job ahead of the first runnable one. Keep the three predicates here in step
-- with the index predicate; drifting apart silently reintroduces that walk.
--
-- Note the consequence: a 'failed' or 'discarded' parent blocks its children
-- permanently. That is the correct reading of a dependency (the workflow is
-- stuck and wants an operator), and cancel_cascade already discards descendants
-- rather than stranding them.
--
-- The candidate set MUST be a CTE, not an `id IN (SELECT ... LIMIT $2)`
-- subquery. As a subquery the planner puts it on the inner side of a nested
-- loop semi join and rescans it once per outer row; because each rescan sees
-- the rows this same statement has already flipped to 'executing', it hands
-- back the next available job every time and the semi join matches all of
-- them. LIMIT then bounds each rescan rather than the statement, and a worker
-- asking for one job checks out the entire queue. A CTE containing a locking
-- clause cannot be inlined, so it is evaluated exactly once and LIMIT means
-- what it says. Verify with EXPLAIN ANALYZE: the Limit node must report
-- loops=1.
WITH candidate AS (
    SELECT
        candidate_job.id
    FROM
        ergon.jobs AS candidate_job
    WHERE
        candidate_job.queue = $1
        AND candidate_job.state = 'available'
        AND candidate_job.scheduled_at <= NOW()
        AND UPPER(candidate_job.valid_period) = 'infinity'
        AND candidate_job.pending_parents = 0
    ORDER BY
        candidate_job.scheduled_at
    FOR UPDATE
        SKIP LOCKED
    LIMIT $2)
UPDATE
    ergon.jobs AS job
SET
    state = 'executing',
    attempt = attempt + 1
FROM
    candidate
WHERE
    job.id = candidate.id
    AND UPPER(job.valid_period) = 'infinity'
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

