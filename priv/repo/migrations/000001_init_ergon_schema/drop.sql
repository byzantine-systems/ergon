-- Reverses create.sql, executed statement-by-statement by
-- Ergon.Repo.Migrations.InitErgonSchema.down.
--
-- Property graphs are not dropped by DROP SCHEMA ... CASCADE, so drop it first.
-- Everything else in the ergon schema (domain, jobs + jobs_history, the RLS
-- policy, triggers, indexes, the jobs_current view, and the ergon-schema
-- functions enqueue/enforce_job_transition/jobs_asof*) is removed by the
-- cascade. The two public-schema functions are dropped explicitly.
DROP PROPERTY GRAPH IF EXISTS ergon.workflow;

DROP SCHEMA IF EXISTS ergon CASCADE;

DROP FUNCTION IF EXISTS pgmq_release_leases(text);
DROP FUNCTION IF EXISTS temporal_versioning();

-- Extension drops (btree_gist, pgcrypto, pgmq, pg_cron) are intentionally not
-- mirrored, they may serve other databases/apps and are cheap to leave.
