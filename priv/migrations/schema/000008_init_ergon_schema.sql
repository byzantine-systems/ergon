-- Ergon relational + graph schema (PostgreSQL 19). The whole initial release
-- reads top-to-bottom here. Class: once (ergon_migrate:sources/1).
--
-- migraterl hands the whole file to epgsql:squery/2, i.e. the simple query
-- protocol, so PostgreSQL's own parser splits the statements. Dollar-quoted
-- ($$) bodies are therefore fine here, and the plpgsql/sql routines are
-- ordinary .sql files under ../functions (before this script) and ../routines
-- (after it), rather than being embedded in the migration runner.
--
-- Prerequisites applied BEFORE this file: ../bootstrap (extensions,
-- CREATE SCHEMA ergon) and ../functions (temporal_versioning() and
-- ergon.enforce_job_transition(), both attached as triggers below).
--
-- One caveat on the file as a whole: migraterl substitutes `$key$` tokens from
-- its `variables` option before executing, and a variable named `foo` would
-- rewrite the dollar-quote tag `$foo$`. ergon_migrate passes no variables and
-- every body here is quoted with a bare `$$`, so the two never collide. Keep
-- it that way.
-- ---------------------------------------------------------------------------
-- job_state: a DOMAIN over text constrained to the supported lifecycle states.
-- Keeps the column human-readable text while making an out-of-set value
-- (e.g. 'banana') a constraint violation at write time, so the set of legal
-- states is owned by the database, not just ergon_job/ergon_fsm.
-- ---------------------------------------------------------------------------
CREATE DOMAIN ergon.job_state AS text CONSTRAINT job_state_valid CHECK (value IN ('available', 'executing', 'completed', 'failed', 'discarded'));

-- ---------------------------------------------------------------------------
-- jobs: the core work table, bi-temporal from creation.
--   * valid_period (application time): when the row is true in the world. It
--     ALWAYS starts [now, infinity). State transitions split it via
--     UPDATE ... FOR PORTION OF (ergon_db:apply_outcome/2). "Live" (the
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
    id bigint GENERATED ALWAYS AS IDENTITY,
    queue text NOT NULL DEFAULT 'default',
    worker text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    state ergon.job_state NOT NULL DEFAULT 'available',
    -- Opt-in tenant discriminator for the row-level security policy below.
    -- Defaults from the `ergon.tenant` GUC so a host running under a tenant
    -- connection tags rows automatically. NULL (GUC unset) means "no tenant",
    -- and the policy then imposes no restriction (single-tenant hosts are
    -- unaffected).
    tenant text DEFAULT nullif (CURRENT_SETTING('ergon.tenant', TRUE), ''),
    -- Deterministic identity of (queue, worker, payload), generated in-DB so it
    -- can never disagree with the columns. Uses pgcrypto's digest(..., 'sha256')
    -- (installed by ../bootstrap/000001_extensions.sql), which is IMMUTABLE as a
    -- generation expression requires (the built-in convert_to is only STABLE, so
    -- sha256(bytea) cannot be used here). Length-prefixing queue/worker keeps the
    -- concatenation unambiguous.
    fingerprint text GENERATED ALWAYS AS (ENCODE(DIGEST(LENGTH(queue)::text || ':' || queue || ':' || LENGTH(worker)::text || ':' || worker || ':' || payload::text, 'sha256'), 'hex')) STORED,
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 20,
    last_error text,
    -- How many of this job's workflow parents have not yet completed. Maintained
    -- by ergon.block_child() and ergon.unblock_children() (../functions), never
    -- written by application code.
    --
    -- It is denormalised onto the row for one reason: jobs_fetch_idx below can
    -- then carry `pending_parents = 0` in its predicate, so a blocked job is not
    -- in the index at all. Expressing the same rule as a join in the checkout
    -- query instead leaves LIMIT bounding the output but not the scan, and the
    -- index walk has to step over every blocked job ahead of the first runnable
    -- one. Measured on PG 19beta2, 200k jobs with 50k blocked at the head of the
    -- queue, checkout of a single job:
    --
    --   live anti-join in checkout          237 ms   253k buffers   50001 scanned
    --   anti-join against a materialised
    --     blocked set (the ceiling an IMMV
    --     could reach)                       42 ms   102k buffers   50001 scanned
    --   this column, inside the index      0.14 ms      9 buffers       1 scanned
    --
    -- The middle row is why this is a column and not an incrementally maintained
    -- materialized view: a view lands in its own relation, so the scan depth is
    -- unchanged and only the per-row probe gets cheaper.
    pending_parents integer NOT NULL DEFAULT 0,
    scheduled_at timestamptz NOT NULL DEFAULT NOW(),
    inserted_at timestamptz NOT NULL DEFAULT NOW(),
    valid_period tstzrange NOT NULL DEFAULT TSTZRANGE(NOW(), 'infinity', '[)'),
    dedup_period tstzrange NOT NULL DEFAULT 'empty',
    system_time tstzrange NOT NULL DEFAULT TSTZRANGE(STATEMENT_TIMESTAMP(), 'infinity', '[)'),
    -- Generated liveness flag: the current version of a job. Readable in
    -- queries/views. The fetch path filters on the underlying expression so the
    -- planner can use the partial index.
    is_live boolean GENERATED ALWAYS AS (UPPER(valid_period) = 'infinity') STORED,
    -- Sanity bounds owned by the database.
    CONSTRAINT jobs_attempt_bounds CHECK (attempt >= 0 AND attempt <= max_attempts),
    CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0),
    -- Deliberately not clamped in the triggers. pending_parents is derived, and
    -- derived state drifts; a bug should surface here as a constraint violation
    -- rather than as a job that is silently unrunnable forever.
    CONSTRAINT jobs_pending_parents_non_negative CHECK (pending_parents >= 0),
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
    EXCLUDE USING gist ((COALESCE(tenant, '')
) WITH =, fingerprint WITH =, dedup_period WITH &&)
WHERE (UPPER(valid_period) = 'infinity')
);

-- Fetch path for ergon_db:checkout/2, partial so it stays proportional to the
-- live-and-runnable frontier rather than growing with valid-time history.
--
-- `pending_parents = 0` belongs in the predicate, not in the query. It is what
-- keeps a job whose workflow parents are unfinished out of the index altogether,
-- so checkout never walks over blocked work to reach runnable work. See the
-- column comment above for the measurements.
CREATE INDEX jobs_fetch_idx ON ergon.jobs (queue, scheduled_at)
WHERE
    state = 'available' AND UPPER(valid_period) = 'infinity' AND pending_parents = 0;

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
CREATE INDEX jobs_history_id_system_time_idx ON ergon.jobs_history USING gist (id, system_time);

-- ---------------------------------------------------------------------------
-- Autovacuum, tuned for churn rather than for size.
--
-- The defaults assume a table whose rows mostly sit still: vacuum once 20% of
-- them are dead. ergon.jobs is the opposite. A single job's normal life writes a
-- new row version on checkout, another on each retry, and another on completion,
-- and because apply_outcome.sql transitions state with FOR PORTION OF, a
-- completed job leaves a superseded version behind as well. Dead tuples
-- therefore accumulate in proportion to throughput, not to table size, and at a
-- steady 1000 jobs/sec a queue holding a few thousand live rows turns over its
-- entire visible content many times a minute.
--
-- Waiting for 20% dead is the wrong trigger for that shape: the fetch path is a
-- partial index scan, and every dead tuple it steps over is work the LIMIT does
-- not bound. Vacuuming at 2% plus a low fixed threshold keeps the frontier the
-- index scan walks close to the live set. The cost is more frequent vacuums on a
-- small table, which is cheap precisely because the table is small.
--
-- ANALYZE runs more often for a related reason: scheduled_at and state
-- distributions shift constantly as jobs move through, and a stale estimate is
-- what talks the planner out of the partial index.
ALTER TABLE ergon.jobs SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 100, autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold = 100);

-- History is append-only: rows are inserted by the versioning trigger and never
-- updated or deleted, so there are no dead tuples to chase and the vacuum
-- defaults are right. It still wants frequent ANALYZE, because it grows without
-- bound and the time-travel queries are range scans over system_time whose plans
-- depend on that estimate.
ALTER TABLE ergon.jobs_history SET (autovacuum_analyze_scale_factor = 0.05);

-- system_time versioning: archive superseded rows into ergon.jobs_history.
CREATE TRIGGER jobs_versioning_trigger
    BEFORE INSERT OR UPDATE OR DELETE ON ergon.jobs
    FOR EACH ROW
    EXECUTE FUNCTION ergon.temporal_versioning ();

-- Defense-in-depth: reject illegal state transitions at write time,
-- regardless of caller.
-- ergon_fsm remains the client-side fast path, this is the authority.
CREATE TRIGGER jobs_transition_guard
    BEFORE UPDATE ON ergon.jobs
    FOR EACH ROW
    EXECUTE FUNCTION ergon.enforce_job_transition ();

-- Reactive wake-ups for ergon_job_notifier are NOT a trigger here. Writers
-- never call pg_notify: every transaction with a pending NOTIFY takes the
-- global notification-queue lock at commit, serializing all notifying
-- commits (the LISTEN/NOTIFY scalability trap). Instead a pg_cron tick runs
-- ergon.notify_pending_jobs() every second, which notifies once per queue
-- that has immediately runnable work (available + due + live). The function
-- is ../routines/000010_notify_pending_jobs.sql (it reads ergon.jobs, so it
-- must come after this file); the tick that calls it is
-- ../cron/000014_job_notify_cron.sql.
-- ---------------------------------------------------------------------------
-- Opt-in multi-tenant isolation. Enabled+FORCED so it applies even to the table
-- owner, but the policy is a no-op when the `ergon.tenant` GUC is unset, so
-- single-tenant hosts see no change. Takes effect only for non-superuser roles
-- without BYPASSRLS, connect Ergon as such a role to actually isolate tenants.
ALTER TABLE ergon.jobs ENABLE ROW LEVEL SECURITY;

ALTER TABLE ergon.jobs FORCE ROW LEVEL SECURITY;

CREATE POLICY jobs_tenant_isolation ON ergon.jobs
    USING (nullif (CURRENT_SETTING('ergon.tenant', TRUE), '') IS NULL
        OR tenant = nullif (CURRENT_SETTING('ergon.tenant', TRUE), ''))
    WITH CHECK (nullif (CURRENT_SETTING('ergon.tenant', TRUE), '') IS NULL
    OR tenant = nullif (CURRENT_SETTING('ergon.tenant', TRUE), ''));

-- ---------------------------------------------------------------------------
-- job_edges: DAG dependencies between jobs (parent triggers child).
-- ---------------------------------------------------------------------------
CREATE TABLE ergon.job_edges (
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    edge_type text NOT NULL DEFAULT 'triggers',
    PRIMARY KEY (parent_id, child_id)
);

CREATE INDEX job_edges_child_idx ON ergon.job_edges (child_id);

-- ---------------------------------------------------------------------------
-- pgmq_notify_queues: which pgmq queues ergon_pgmq_consumer wants wake-ups for.
--
-- pgmq is the other queue in this library: a durable message transport, separate
-- from ergon.jobs, that host applications create their own queues on. Its
-- consumers get the same reactive fast path the job workers get, and for the
-- same reason it is a single pg_cron tick rather than per-queue triggers.
--
-- This table exists so that tick can be ONE tick. The obvious design installs a
-- cron job per pgmq queue, but each one is a separate notifying transaction and
-- so a separate acquisition of the global notification-queue lock at commit,
-- precisely the contention that makes trigger-per-insert plateau around 2.9K
-- writes/sec while consuming no CPU, memory, or IOPS. Registering queues here
-- instead lets ergon.notify_pending_pgmq() emit one pg_notify per queue holding
-- visible work from a single transaction, taking the lock once no matter how
-- many queues exist. That mirrors ergon.notify_pending_jobs() exactly.
--
-- A registry rather than scanning pgmq.list_queues(): other applications may
-- share this database and own pgmq queues of their own, and notifying on a
-- channel derived from their queue names would be both wasteful and liable to
-- collide with their own naming. Ergon notifies only for queues it was told
-- about.
--
-- channel is stored rather than derived so it reaches pg_notify as a column
-- value, i.e. as data rather than as interpolated DDL.
-- ---------------------------------------------------------------------------
CREATE TABLE ergon.pgmq_notify_queues (
    queue_name text PRIMARY KEY,
    channel text NOT NULL
);

-- Keep ergon.jobs.pending_parents in step with the edges. Adding an edge to a
-- parent that has not completed blocks the child; removing it unblocks. Both
-- directions are in ergon.block_child (../functions).
CREATE TRIGGER job_edges_block_child_trigger
    AFTER INSERT OR DELETE ON ergon.job_edges
    FOR EACH ROW
    EXECUTE FUNCTION ergon.block_child ();

-- The other half: a parent reaching 'completed' decrements every child.
--
-- The WHEN clause is doing real work. apply_outcome.sql transitions state with
-- UPDATE ... FOR PORTION OF, which both updates the live row and writes the
-- superseded portion as its own row, so restricting to upper(valid_period) =
-- 'infinity' is what keeps this firing once rather than twice. Requiring the
-- state to actually change stops a no-op update from decrementing again, and
-- also means the pending_parents update inside the function cannot re-enter
-- this trigger.
CREATE TRIGGER jobs_unblock_children_trigger
    AFTER UPDATE ON ergon.jobs
    FOR EACH ROW
    WHEN (NEW.state = 'completed' AND OLD.state IS DISTINCT FROM NEW.state AND UPPER(NEW.valid_period) = 'infinity')
    EXECUTE FUNCTION ergon.unblock_children ();

-- Convenience view: the current version of every job, one row per id. Declared
-- before the property graph below, which uses it as its vertex table.
CREATE VIEW ergon.jobs_current AS
SELECT
    *
FROM
    ergon.jobs
WHERE
    UPPER(valid_period) = 'infinity';

-- ---------------------------------------------------------------------------
-- workflow: PostgreSQL 19 SQL/PGQ property graph over jobs + job_edges. Used
-- for single-hop dependency resolution (ergon_graph:ready_children/0,
-- direct_children/1). Multi-hop reachability (descendants, cycle detection,
-- cascade-cancel) uses recursive CTEs over job_edges instead, PG19's SQL/PGQ
-- does not yet support path quantifiers.
--
-- The vertex table is ergon.jobs_current, NOT ergon.jobs, and the distinction is
-- important. `KEY (id)` claims id identifies a row, but id is deliberately not
-- unique in ergon.jobs: the temporal primary key is
-- (id, valid_period WITHOUT OVERLAPS), so a job owns one row per valid-time
-- version and every version would become its own vertex. Since apply_outcome.sql
-- transitions state with FOR PORTION OF, a completed job always has at least two
-- versions, and the graph would then report a parent as both 'completed' and
-- whatever it was before:
--
--   parent_id | parent_state | child_id
--   ----------+--------------+----------
--          63 | completed    |       64
--          63 | executing    |       64
--
-- ready_children.sql keeps children where bool_and(parent_state = 'completed'),
-- so the superseded vertex would make that false and the query would return
-- nothing for every parent that had ever run. Filtering to live rows at the
-- vertex table makes id unique and the KEY honest. PostgreSQL allows this:
-- element tables may be views, not only base tables.
-- ---------------------------------------------------------------------------
CREATE PROPERTY GRAPH ergon.workflow VERTEX TABLES (
    ergon.jobs_current AS jobs KEY (id) LABEL job PROPERTIES (id, state, worker, queue)) EDGE TABLES (
    ergon.job_edges AS edges KEY (parent_id, child_id) SOURCE KEY (parent_id) REFERENCES jobs (id) DESTINATION KEY (child_id) REFERENCES jobs (id) LABEL triggers PROPERTIES (edge_type)
);

