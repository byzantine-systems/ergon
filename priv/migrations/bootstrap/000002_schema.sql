-- Ergon's own schema. Class: once (ergon_migrate:sources/1).
--
-- Split out from the table DDL in schema/ because the schema-qualified
-- functions in functions/ (ergon.enforce_job_transition) must be creatable
-- before any table exists.
CREATE SCHEMA IF NOT EXISTS ergon;

