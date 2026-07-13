defmodule Ergon.Repo.Migrations.InitErgonSchema do
  @moduledoc """
  Ergon core schema, PostgreSQL 18/19. The relational + graph schema, the
  `job_state` domain, the bi-temporal `jobs` + `jobs_history` tables (generated
  `fingerprint`/`is_live`, temporal PK, partial per-tenant uniqueness EXCLUDE,
  RLS policy), the `job_edges` table, and the SQL/PGQ `workflow` property graph,
  is created by the single colocated `000001_init_ergon_schema/create.sql`
  (reversed by `drop.sql`), so the whole schema reads top-to-bottom in one file.

  `ergon.jobs` is bi-temporal from creation, its `system_time` column and
  versioning trigger are defined in `create.sql` directly, with no
  ALTER/backfill step.

  Raw SQL is used because Ecto's schema DSL cannot express PostgreSQL 18
  temporal keys (`PRIMARY KEY (id, valid_period WITHOUT OVERLAPS)`), a
  `tstzrange` validity column, or a SQL/PGQ property graph. `execute_file/1`
  from Ecto sends a whole file as one query, but Postgrex runs one statement
  per query, so `execute_sql_file/1` below splits the script on semicolons and
  executes each statement in order. `create.sql`/`drop.sql` are therefore kept
  free of dollar-quoted (`$$`) bodies, the plpgsql functions are installed as
  single `execute/1` statements from here, before the schema when a trigger in
  `create.sql` references them (`temporal_versioning/0` via
  `Ergon.Migration.versioning_trigger/0`, `ergon.enforce_job_transition/0`, and
  `ergon.job_notify/0` for the `Ergon.JobNotifier` wake-up trigger),
  after it otherwise (`ergon.enqueue`, `pgmq_release_leases`, `ergon.jobs_asof*`).
  Once applied, treat the paired `.sql` + `.exs` as immutable (Ecto tracks
  migrations by version, not content).
  """
  use Ecto.Migration

  import Ergon.Migration

  def up do
    # btree_gist + pgcrypto (temporal keys, exclusion, generated fingerprint),
    # pgmq, and conditionally pg_cron (only in the cron database, skipped in test).
    extensions()

    execute("CREATE SCHEMA IF NOT EXISTS ergon")

    # Shared, generic bi-temporal versioning function. `create.sql` attaches it
    # to ergon.jobs via a trigger, so it must exist first.
    versioning_trigger()

    # Transition guard (attached as a BEFORE UPDATE trigger in create.sql, so it
    # must exist first). Defense-in-depth: rejects any illegal state transition
    # at write time regardless of caller, while Ergon.FSM stays the client-side
    # fast path. The legal edges mirror Ergon.FSM.transition/2 exactly.
    execute("""
    CREATE FUNCTION ergon.enforce_job_transition() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.state IS DISTINCT FROM OLD.state
         AND (OLD.state, NEW.state) NOT IN (
           ('available', 'executing'),
           ('executing', 'completed'),
           ('executing', 'failed'),
           ('executing', 'available'),
           ('available', 'discarded'),
           ('executing', 'discarded')
         ) THEN
        RAISE EXCEPTION 'illegal job transition: % -> %', OLD.state, NEW.state
          USING ERRCODE = 'check_violation';
      END IF;
      RETURN NEW;
    END
    $$
    """)

    # Wake-up trigger function for Ergon.JobNotifier (attached as an AFTER
    # INSERT OR UPDATE trigger in create.sql, guarded to runnable rows, so it
    # must exist first). Fires pg_notify on the fixed channel with the queue
    # name only, the payload is never job data or tenant (NOTIFY bypasses RLS).
    # Reusable half packaged as Ergon.Migration.job_notify_trigger/0 for parity
    # with pgmq_notify_trigger/2, here we install just the function ($$ body),
    # create.sql owns the guarded trigger attachment.
    execute("""
    CREATE FUNCTION ergon.job_notify() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      PERFORM pg_notify('#{Ergon.JobNotifier.channel()}', NEW.queue);
      RETURN NEW;
    END
    $$
    """)

    execute_sql_file(sql_path("create.sql"))

    # ergon.enqueue: get-or-create insert. Inserts the job and, on the temporal
    # uniqueness EXCLUDE violation, returns the existing live overlapping job
    # instead of raising, so a duplicate unique job is a no-op that returns the
    # incumbent (Ergon.DB.insert/1). p_dedup_seconds = 0 means non-unique (an
    # empty dedup_period that never overlaps).
    execute("""
    CREATE FUNCTION ergon.enqueue(
      p_queue text, p_worker text, p_payload jsonb, p_max_attempts int, p_dedup_seconds int
    ) RETURNS SETOF ergon.jobs
    LANGUAGE plpgsql AS $$
    DECLARE
      v_dedup tstzrange;
    BEGIN
      v_dedup := CASE
        WHEN p_dedup_seconds > 0
          THEN tstzrange(now(), now() + make_interval(secs => p_dedup_seconds), '[)')
        ELSE 'empty'::tstzrange
      END;

      RETURN QUERY
        INSERT INTO ergon.jobs (queue, worker, payload, max_attempts, dedup_period)
        VALUES (p_queue, p_worker, p_payload, p_max_attempts, v_dedup)
        RETURNING *;
    EXCEPTION WHEN exclusion_violation THEN
      RETURN QUERY
        SELECT * FROM ergon.jobs j
        WHERE j.queue = p_queue
          AND j.worker = p_worker
          AND j.payload = p_payload
          AND j.dedup_period && v_dedup
          AND upper(j.valid_period) = 'infinity'
        LIMIT 1;
    END
    $$
    """)

    # Time-travel readers over the bi-temporal history (Ergon.DB.jobs_asof/1,
    # jobs_asof_system/1). jobs_asof: the application-time truth at instant t.
    # jobs_asof_system: what the DB believed at t, spanning live rows and the
    # archived jobs_history twin.
    execute("""
    CREATE FUNCTION ergon.jobs_asof(t timestamptz) RETURNS SETOF ergon.jobs
    LANGUAGE sql STABLE AS $$
      SELECT * FROM ergon.jobs WHERE valid_period @> t
    $$
    """)

    execute("""
    CREATE FUNCTION ergon.jobs_asof_system(t timestamptz) RETURNS SETOF ergon.jobs
    LANGUAGE sql STABLE AS $$
      SELECT * FROM ergon.jobs WHERE system_time @> t
      UNION ALL
      SELECT * FROM ergon.jobs_history WHERE system_time @> t
    $$
    """)

    # pgmq_release_leases(queue): force-expire every in-flight visibility lease
    # on a queue. plpgsql because the queue table name (pgmq.q_<queue>) cannot
    # be parameterised. Called by Ergon.Pgmq.release_leases/1 and the future
    # Reconciler. Ergon owns no queues itself, this only installs the helper
    # that operates on whatever queues a host creates via
    # Ergon.Migration.pgmq_queue/1.
    execute("""
    CREATE FUNCTION pgmq_release_leases(queue_name text) RETURNS bigint
    LANGUAGE plpgsql AS $$
    DECLARE
      released bigint;
    BEGIN
      EXECUTE format(
        'UPDATE pgmq.q_%I SET vt = clock_timestamp() WHERE vt > clock_timestamp()',
        queue_name);
      GET DIAGNOSTICS released = ROW_COUNT;
      RETURN released;
    END
    $$
    """)
  end

  def down do
    # drop.sql drops the ergon schema CASCADE (taking enqueue, jobs_asof*,
    # enforce_job_transition with it) plus the public-schema functions.
    execute_sql_file(sql_path("drop.sql"))

    # Extension drops are intentionally not mirrored, pgmq/pg_cron may serve
    # other databases/apps, and btree_gist/pgcrypto are cheap to leave installed.
  end

  # Resolves a `.sql` file colocated with this migration. `__DIR__` is the
  # migrations directory: Ecto compiles migration files from disk at
  # migration-run time, so this is correct under `mix ecto.migrate` and in
  # releases (priv ships with the app) alike.
  defp sql_path(name), do: Path.join([__DIR__, "000001_init_ergon_schema", name])

  # Reads a `.sql` script and executes each statement individually, because
  # Postgrex runs a single statement per query. The scripts contain no
  # dollar-quoted bodies, so splitting on `;` is sufficient.
  defp execute_sql_file(path) do
    path
    |> File.read!()
    |> String.split(";")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.each(&execute/1)
  end
end
