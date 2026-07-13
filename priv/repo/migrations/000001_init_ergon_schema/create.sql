-- Ergon relational + graph schema (PostgreSQL 18/19). The whole initial release
-- reads top-to-bottom here. Loaded by Ergon.Repo.Migrations.InitErgonSchema,
-- which splits this script on semicolons and executes each statement in order
-- (Postgrex runs one statement per query). Keep this file free of dollar-quoted
-- ($$) bodies so the simple split stays correct. The plpgsql functions
-- (temporal_versioning, enforce_job_transition, job_notify, enqueue,
-- pgmq_release_leases, jobs_asof*) are installed from the migration module
-- instead.
--
-- Prerequisites installed by the migration BEFORE this file runs: the
-- extensions (btree_gist, pgcrypto, pgmq, conditionally pg_cron), `CREATE
-- SCHEMA ergon`,
-- the shared temporal_versioning() function, and ergon.enforce_job_transition()
-- (both attached as triggers below).

-- ---------------------------------------------------------------------------
-- job_state: a DOMAIN over text constrained to the supported lifecycle states.
-- Keeps the column human-readable text while making an out-of-set value
-- (e.g. 'banana') a constraint violation at write time, so the set of legal
-- states is owned by the database, not just Ergon.Job/Ergon.FSM.
-- ---------------------------------------------------------------------------
CREATE DOMAIN ergon.job_state AS text
CONSTRAINT job_state_valid
CHECK (value IN ('available', 'executing', 'completed', 'failed', 'discarded'));

-- ---------------------------------------------------------------------------
-- jobs: the core work table, bi-temporal from creation.
--   * valid_period (application time): when the row is true in the world. It
--     ALWAYS starts [now, infinity). State transitions split it via
--     UPDATE ... FOR PORTION OF (Ergon.DB.apply_outcome/2). "Live" (the 
--     current version) means upper(valid_period) = 'infinity', surfaced as 
--     is_live.
--   * system_time: when the database believes the row, maintained by the 
--     temporal_versioning() trigger + ergon.jobs_history.
--   * dedup_period: the uniqueness window (separate from valid_period so 
--     unique jobs remain checkoutable). 'empty' for non-unique jobs (empty 
--     ranges never overlap, so duplicates coexist). [now, now+N) for 
--     unique-for-N jobs.
-- ---------------------------------------------------------------------------
CREATE TABLE ergon.jobs (
id BIGINT GENERATED ALWAYS AS IDENTITY,
queue TEXT NOT NULL DEFAULT 'default',
worker TEXT NOT NULL,
payload JSONB NOT NULL DEFAULT '{}'::JSONB,
state ergon.job_state NOT NULL DEFAULT 'available',
-- Opt-in tenant discriminator for the row-level security policy below.
-- Defaults from the `ergon.tenant` GUC so a host running under a tenant
-- connection tags rows automatically. NULL (GUC unset) means "no tenant",
-- and the policy then imposes no restriction (single-tenant hosts are
-- unaffected).
tenant TEXT DEFAULT nullif(current_setting('ergon.tenant', true), ''),
-- Deterministic identity of (queue, worker, payload), generated in-DB so it
-- can never disagree with the columns. Uses pgcrypto's digest(..., 'sha256')
-- (installed by Ergon.Migration.extensions/0), which is IMMUTABLE as a
-- generation expression requires (the built-in convert_to is only STABLE, so
-- sha256(bytea) cannot be used here). Length-prefixing queue/worker keeps the
-- concatenation unambiguous.
fingerprint TEXT GENERATED ALWAYS AS (
encode(
digest(
length(queue)::text || ':' || queue || ':' ||
length(worker)::text || ':' || worker || ':' || payload::text,
'sha256'
),
'hex'
)
) STORED,
attempt INTEGER NOT NULL DEFAULT 0,
max_attempts INTEGER NOT NULL DEFAULT 20,
last_error TEXT,
scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
valid_period TSTZRANGE NOT NULL DEFAULT tstzrange(now(), 'infinity', '[)'),
dedup_period TSTZRANGE NOT NULL DEFAULT 'empty',
system_time TSTZRANGE NOT NULL DEFAULT tstzrange(now(), NULL),
-- Generated liveness flag: the current version of a job. Readable in
-- queries/views. The fetch path filters on the underlying expression so the
-- planner can use the partial index.
is_live BOOLEAN GENERATED ALWAYS AS (upper(valid_period) = 'infinity') STORED,
-- Sanity bounds owned by the database.
CONSTRAINT jobs_attempt_bounds CHECK (attempt >= 0 AND attempt <= max_attempts),
CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0),
-- Temporal PK: an id is unique at any instant, but the same id may own
-- several non-overlapping historical valid_period versions.
PRIMARY KEY (id, valid_period WITHOUT OVERLAPS),
-- Windowed uniqueness among LIVE rows only. Written as an explicit partial
-- EXCLUDE (rather than UNIQUE ... WITHOUT OVERLAPS) because the partial
-- predicate is what lets it coexist with FOR PORTION OF: a superseded
-- version keeps the same fingerprint+dedup_period, and without the
-- `WHERE upper(valid_period)='infinity'` guard the split would self-conflict.
-- coalesce(tenant,'') scopes uniqueness per tenant while still applying to
-- untenanted (NULL) rows.
CONSTRAINT jobs_unique_fingerprint
EXCLUDE USING gist (
(coalesce(tenant, '')) WITH =,
fingerprint WITH =,
dedup_period WITH &&
)
WHERE (upper(valid_period) = 'infinity')
);

-- Fetch path for Ergon.DB.checkout/2, partial so it stays proportional to the
-- live-and-available frontier rather than growing with valid-time history.
CREATE INDEX jobs_fetch_idx ON ergon.jobs (queue, scheduled_at)
WHERE state = 'available' AND upper(valid_period) = 'infinity';

-- History twin for system-time versioning. LIKE ... INCLUDING DEFAULTS
-- INCLUDING CONSTRAINTS copies columns/CHECK/NOT NULL but NOT the 
-- temporal PK/EXCLUDE (those need INCLUDING INDEXES) and NOT the 
-- generation expressions (those need INCLUDING GENERATED) both 
-- deliberately omitted: history is an append-only audit log the trigger
-- writes verbatim via `INSERT ... SELECT (old).*`, so 
-- fingerprint/is_live must be plain writable columns here.
CREATE TABLE ergon.jobs_history (
LIKE ergon.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);

-- Time-travel index: "what did the DB believe about job X as of time T?"
CREATE INDEX jobs_history_id_system_time_idx
ON ergon.jobs_history USING gist (id, system_time);

-- system_time versioning: archive superseded rows into ergon.jobs_history.
CREATE TRIGGER jobs_versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON ergon.jobs
FOR EACH ROW EXECUTE FUNCTION temporal_versioning();

-- Defense-in-depth: reject illegal state transitions at write time, 
-- regardless of caller. 
-- Ergon.FSM remains the client-side fast path, this is the authority.
CREATE TRIGGER jobs_transition_guard
BEFORE UPDATE ON ergon.jobs
FOR EACH ROW EXECUTE FUNCTION ergon.enforce_job_transition();

-- Reactive wake-up for Ergon.JobNotifier: fire pg_notify on the 
-- fixed channel with the queue name whenever a row becomes 
-- immediately runnable (available + due + live). 
-- Checkout (-> executing) and future-scheduled retries are guarded
-- out, so only rows a worker could claim right now wake anyone. 
-- ergon.job_notify has a $$ body and so is installed from the 
-- migration module.
CREATE TRIGGER jobs_notify_trigger
AFTER INSERT OR UPDATE ON ergon.jobs
FOR EACH ROW
WHEN (
NEW.state = 'available'
AND NEW.scheduled_at <= now()
AND upper(NEW.valid_period) = 'infinity'
)
EXECUTE FUNCTION ergon.job_notify();

-- Opt-in multi-tenant isolation. Enabled+FORCED so it applies even to the table
-- owner, but the policy is a no-op when the `ergon.tenant` GUC is unset, so
-- single-tenant hosts see no change. Takes effect only for non-superuser roles
-- without BYPASSRLS, connect Ergon as such a role to actually isolate tenants.
ALTER TABLE ergon.jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE ergon.jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY jobs_tenant_isolation ON ergon.jobs
USING (
nullif(current_setting('ergon.tenant', true), '') IS NULL
OR tenant = nullif(current_setting('ergon.tenant', true), '')
)
WITH CHECK (
nullif(current_setting('ergon.tenant', true), '') IS NULL
OR tenant = nullif(current_setting('ergon.tenant', true), '')
);

-- ---------------------------------------------------------------------------
-- job_edges: DAG dependencies between jobs (parent triggers child).
-- ---------------------------------------------------------------------------
CREATE TABLE ergon.job_edges (
parent_id BIGINT NOT NULL,
child_id BIGINT NOT NULL,
edge_type TEXT NOT NULL DEFAULT 'triggers',
PRIMARY KEY (parent_id, child_id)
);

CREATE INDEX job_edges_child_idx ON ergon.job_edges (child_id);

-- ---------------------------------------------------------------------------
-- workflow: PostgreSQL 19 SQL/PGQ property graph over jobs + job_edges. Used
-- for single-hop dependency resolution (Ergon.Graph.ready_children/0,
-- direct_children/1). Multi-hop reachability (descendants, cycle detection,
-- cascade-cancel) uses recursive CTEs over job_edges instead, PG19's SQL/PGQ
-- does not yet support path quantifiers.
-- ---------------------------------------------------------------------------
CREATE PROPERTY GRAPH ergon.workflow
VERTEX TABLES (
ergon.jobs AS jobs
KEY (id)
LABEL job PROPERTIES (id, state, worker, queue)
)
EDGE TABLES (
ergon.job_edges AS edges
KEY (parent_id, child_id)
SOURCE KEY (parent_id) REFERENCES jobs (id)
DESTINATION KEY (child_id) REFERENCES jobs (id)
LABEL triggers PROPERTIES (edge_type)
);

-- Convenience view: the current version of every job (one row per id).
CREATE VIEW ergon.jobs_current AS
SELECT * FROM ergon.jobs WHERE upper(valid_period) = 'infinity';
