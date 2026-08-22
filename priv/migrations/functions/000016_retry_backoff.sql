-- retry_backoff(attempt, base_ms, cap_ms, strategy): how long an errored job
-- waits before it is runnable again. Class: on_change (ergon_migrate:sources/1).
--
-- The formulas are from Marc Brooker, "Exponential Backoff and Jitter", AWS
-- Architecture Blog, 4 March 2015:
--
--   https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
--
-- Given ceiling = min(cap, base * 2^(attempt - 1)):
--
--   none          ceiling                            the article's baseline
--   full_jitter   random(0, ceiling)                 the article's recommendation
--   equal_jitter  ceiling/2 + random(0, ceiling/2)
--
-- Jitter is the whole point. Without it every job that errors in the same second
-- is rescheduled to the same instant, and since the notify tick is
-- level-triggered on "runnable work exists", the first tick past that instant
-- wakes every worker on the queue at once. A batch that fails together retries
-- together, fails together again, and re-synchronises on a longer interval.
-- Past the knee it is worse still: an unjittered exponential pinned at the cap
-- keeps the whole batch aligned forever, since min(cap, ...) is the same value
-- for every row.
--
-- Decorrelated Jitter, the article's fourth variant, is deliberately absent. It
-- is defined as min(cap, random(base, previous_sleep * 3)) and so needs the
-- previous sleep carried across attempts, which here would mean a column on
-- ergon.jobs. The article's own simulation measures it as no better than Full
-- Jitter, so the column would buy nothing.
--
-- The article writes the exponent as 2^attempt for a counter starting at zero.
-- Ergon's starts at one: checkout.sql increments `attempt` inside the statement
-- that locks the job, so the first failure reaches apply_outcome with
-- attempt = 1. Hence 2^(attempt - 1), which puts the first retry's ceiling at
-- exactly `base`.
--
-- VOLATILE PARALLEL RESTRICTED, both inherited from random() rather than chosen:
-- PostgreSQL does not check that a function's markings are consistent with what
-- its body calls, so getting them wrong is silent.
--
-- VOLATILE because the body returns a different answer every time. Marking it
-- STABLE would not change today's behaviour, since PostgreSQL does not memoise
-- STABLE calls, but it would license optimisations that assume a determinism
-- this does not have and permit the function somewhere it must never appear,
-- such as an index expression.
--
-- PARALLEL RESTRICTED because random() is: its state is backend-local, so a
-- parallel worker does not share the leader's sequence. This costs nothing here.
-- The only caller is a single-row UPDATE, and PostgreSQL does not parallelise
-- the writing side of DML at all.
--
-- Neither marking costs an index, since every call site puts this in an UPDATE
-- target list rather than a WHERE clause.
--
-- Explicitly `ergon.`, never bare: every object Ergon owns lives in the ergon
-- schema, and a bare name would land wherever the connecting role's search_path
-- happens to point.
CREATE OR REPLACE FUNCTION ergon.retry_backoff (attempt integer, base_ms integer DEFAULT 1000, cap_ms integer DEFAULT 100000, strategy text DEFAULT 'full_jitter')
    RETURNS interval
    LANGUAGE sql
    VOLATILE PARALLEL RESTRICTED
    AS $$
    -- The ELSE branch is full_jitter, so a caller reaching this function
    -- directly with an unrecognised strategy gets the recommended one rather
    -- than an error. Callers coming through ergon_db cannot get here: the atom
    -- is clause-matched on the Erlang side and a typo fails there.
    SELECT
        MAKE_INTERVAL(secs => (
                CASE strategy
                WHEN 'none' THEN
                    ceiling_ms
                WHEN 'equal_jitter' THEN
                    ceiling_ms / 2.0 + RANDOM() * ceiling_ms / 2.0
                ELSE
                    RANDOM() * ceiling_ms
                END) / 1000.0)
    FROM (
        -- Shifted rather than exponentiated: base * 2^(attempt - 1) exactly, in
        -- bigint, with no float rounding anywhere near the ceiling. The exponent
        -- is clamped at 30 so no max_attempts a host can set overflows it, and
        -- GREATEST guards attempt = 0, which apply_outcome never passes but the
        -- jobs_attempt_bounds CHECK permits.
        SELECT
            LEAST (cap_ms::bigint, base_ms::bigint << LEAST (GREATEST (attempt - 1, 0), 30)) AS ceiling_ms) AS ceiling;
$$;

