-- Application-time reader: the truth about every job at instant t.
-- Class: on_change (ergon_migrate:sources/1). Applies after schema/.
--
-- Backs ergon_db:jobs_asof/1. valid_period @> t selects the version whose
-- validity window contains t, so a job that has since transitioned still
-- reports the state it held then.
CREATE OR REPLACE FUNCTION ergon.jobs_asof (t timestamptz)
    RETURNS SETOF ergon.jobs
    LANGUAGE sql
    STABLE
    AS $$
    SELECT
        *
    FROM
        ergon.jobs
    WHERE
        valid_period @> t
$$;

