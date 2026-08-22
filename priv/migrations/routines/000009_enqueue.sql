-- ergon.enqueue: get-or-create insert. Class: on_change
-- (ergon_migrate:sources/1). Applies after schema/: its signature names
-- ergon.jobs as a return type, so the table must already exist.
--
-- Inserts the job and, on the temporal uniqueness EXCLUDE violation, returns
-- the existing live overlapping job instead of raising, so a duplicate unique
-- job is a no-op that returns the incumbent (ergon_db:insert/1).
-- p_dedup_seconds = 0 means non-unique: an empty dedup_period, and empty
-- ranges never overlap, so duplicates coexist.
CREATE OR REPLACE FUNCTION ergon.enqueue (p_queue text, p_worker text, p_payload jsonb, p_max_attempts int, p_dedup_seconds int)
    RETURNS SETOF ergon.jobs
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_dedup tstzrange;
BEGIN
    v_dedup := CASE WHEN p_dedup_seconds > 0 THEN
        TSTZRANGE(NOW(), NOW() + MAKE_INTERVAL(secs => p_dedup_seconds), '[)')
    ELSE
        'empty'::tstzrange
    END;
    RETURN QUERY INSERT INTO ergon.jobs (queue, worker, payload, max_attempts, dedup_period)
        VALUES (p_queue, p_worker, p_payload, p_max_attempts, v_dedup)
    RETURNING
        *;
EXCEPTION
    WHEN exclusion_violation THEN
        RETURN QUERY
        SELECT
            *
        FROM
            ergon.jobs j
        WHERE
            j.queue = p_queue
            AND j.worker = p_worker
            AND j.payload = p_payload
            AND j.dedup_period && v_dedup
            AND UPPER(j.valid_period) = 'infinity'
        LIMIT 1;
END
$$;

