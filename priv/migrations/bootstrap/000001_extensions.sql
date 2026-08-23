-- Extensions Ergon depends on. Class: once (ergon_migrate:sources/1).
--
--   * btree_gist  required for temporal WITHOUT OVERLAPS keys (GiST over
--                 bigint + range composites) and for the mixed
--                 equality/overlap EXCLUDE constraint on ergon.jobs.
--   * pgcrypto    provides the IMMUTABLE digest(..., 'sha256') behind the
--                 generated ergon.jobs.fingerprint column. The built-in
--                 sha256(bytea) cannot be used: convert_to is only STABLE,
--                 and a generation expression requires IMMUTABLE.
--   * pgmq        durable queue transport for ergon_pgmq.
--   * pg_cron     ONLY when the current database matches cron.database_name.
--                 pg_cron can be created in exactly one database per cluster
--                 (set by cron.database_name in postgresql.conf); in any other
--                 database CREATE EXTENSION would fail outright. The guard is
--                 what lets this same script run cleanly against dev (pg_cron
--                 present) and test (pg_cron absent).
--
-- Every statement is IF NOT EXISTS, so this is re-runnable by hand even
-- though migraterl only applies it once.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE EXTENSION IF NOT EXISTS pgmq;

DO $$
BEGIN
    IF CURRENT_DATABASE() = CURRENT_SETTING('cron.database_name', TRUE) THEN
        CREATE EXTENSION IF NOT EXISTS pg_cron;
    END IF;
END
$$;

