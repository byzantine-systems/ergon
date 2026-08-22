-- System-time reader: what the database believed at instant t.
-- Class: on_change (ergon_migrate:sources/1). Applies after schema/.
--
-- Backs ergon_db:jobs_asof_system/1. Spans both the live table and the
-- archived jobs_history twin, since a superseded belief lives only in
-- history. Contrast ergon.jobs_asof, which answers about the world rather
-- than about the database's record of it.
CREATE OR REPLACE FUNCTION ergon.jobs_asof_system (t timestamptz)
    RETURNS SETOF ergon.jobs
    LANGUAGE sql
    STABLE
    AS $$
    SELECT
        *
    FROM
        ergon.jobs
    WHERE
        system_time @> t
    UNION ALL
    SELECT
        *
    FROM
        ergon.jobs_history
    WHERE
        system_time @> t
$$;

