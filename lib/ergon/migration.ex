defmodule Ergon.Migration do
  @moduledoc """
  DDL helpers for host applications consuming Ergon's PostgreSQL patterns.

  `import Ergon.Migration` inside an `Ecto.Migration` to get readable, reversible
  helpers for the generic mechanisms Ergon is built on:

      defmodule MyApp.Repo.Migrations.CreateAssets do
        use Ecto.Migration
        import Ergon.Migration

        def change do
          extensions()
          versioning_trigger()
          bitemporal_table(:assets, "name text NOT NULL, state text NOT NULL DEFAULT 'idle'")
          pgmq_queue(:asset_events)
          pgmq_notify_trigger(:asset_events)
        end
      end

  Each helper calls `Ecto.Migration.execute/{1,2}` directly, so it behaves like
  native Ecto DSL, reversible in `change/0` where it can be, plain `execute/1`
  where it can't (e.g. function definitions have no meaningful "down" beyond
  `DROP FUNCTION`, which the caller can issue explicitly when needed).
  """

  @doc """
  Installs the PostgreSQL extensions Ergon depends on:

    * `btree_gist`, required for temporal `WITHOUT OVERLAPS` keys (GiST over
      bigint + range composites) and the mixed equality/overlap exclusion
      constraint on `ergon.jobs`.
    * `pgcrypto`, provides the IMMUTABLE `digest(..., 'sha256')` used by the
      generated `ergon.jobs.fingerprint` column.
    * `pgmq`, durable queue transport for `Ergon.Pgmq.*` (Phase 3+).
    * `pg_cron`, **only** when the current database matches
      `cron.database_name`. `pg_cron` can be created in exactly one database
      per cluster (set by `cron.database_name` in `postgresql.conf`), and in any
      other database the `CREATE EXTENSION` would fail, so it is skipped. This
      is what lets the same migration run cleanly against dev (where `pg_cron`
      is installed) and test (where it is not).

  This is idempotent, safe to call from every host migration.
  """
  @spec extensions() :: :ok
  def extensions do
    Ecto.Migration.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    Ecto.Migration.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    Ecto.Migration.execute("CREATE EXTENSION IF NOT EXISTS pgmq")

    Ecto.Migration.execute("""
    DO $$
    BEGIN
      IF current_database() = current_setting('cron.database_name', true) THEN
        CREATE EXTENSION IF NOT EXISTS pg_cron;
      END IF;
    END
    $$
    """)

    :ok
  end

  @doc """
  Installs the shared, generic `temporal_versioning()` trigger function (once
  per database). It is column-agnostic, the function inspects `tg_table_name`
  and `tg_table_schema` at fire-time and archives the `OLD` row into
  `<schema>.<table>_history` by naming convention. It is schema-aware (ergon
  keeps its tables in the `ergon` schema with `search_path = public`, so an
  unqualified `<table>_history` would resolve to the wrong schema).

  Any table that wants bi-temporal versioning calls this first (idempotent,'
  `CREATE OR REPLACE`), then attaches the function via:

      CREATE TRIGGER <table>_versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON <table>
        FOR EACH ROW EXECUTE FUNCTION temporal_versioning()

  (The `bitemporal_table/2` helper does both.)

  Uses `clock_timestamp()` rather than `now()`/`transaction_timestamp()` for
  the `system_time` upper bound: `now()` is frozen for the whole transaction,
  so an insert+update in one transaction would close the history row at its
  own lower bound, producing an empty (invisible) `system_time` window.
  """
  @spec versioning_trigger() :: :ok
  def versioning_trigger do
    Ecto.Migration.execute("""
    CREATE OR REPLACE FUNCTION temporal_versioning() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      IF tg_op IN ('UPDATE', 'DELETE') THEN
        old.system_time := tstzrange(lower(old.system_time), clock_timestamp());
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', tg_table_schema, tg_table_name || '_history')
          USING old;
      END IF;

      IF tg_op = 'DELETE' THEN
        RETURN old;
      END IF;

      new.system_time := tstzrange(clock_timestamp(), NULL);
      RETURN new;
    END
    $$
    """)

    :ok
  end

  @doc """
  Creates a bi-temporal table with application-time (`valid_time`) and emulated
  system-time versioning.

  `data_columns` is a SQL fragment for the table's own columns, e.g.
  `"name text NOT NULL, state text NOT NULL DEFAULT 'idle'"`. The helper wraps
  it with the standard temporal scaffolding:

      CREATE SEQUENCE <table>_id_seq
      CREATE TABLE <table> (
        id BIGINT NOT NULL DEFAULT nextval('<table>_id_seq'),
        <data_columns>,
        valid_time   tstzrange NOT NULL DEFAULT tstzrange(now(), NULL),
        system_time  tstzrange NOT NULL DEFAULT tstzrange(now(), NULL),
        PRIMARY KEY (id, valid_time WITHOUT OVERLAPS)
      )
      ALTER SEQUENCE <table>_id_seq OWNED BY <table>.id
      CREATE TABLE <table>_history (LIKE <table> INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
      CREATE INDEX <table>_history_id_system_time_idx
        ON <table>_history USING gist (id, system_time)
      CREATE TRIGGER <table>_versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON <table>
        FOR EACH ROW EXECUTE FUNCTION temporal_versioning()

  Notes:
    * The `id` comes from a **bare sequence**, not `GENERATED ALWAYS AS
      IDENTITY`, the temporal PK means one entity legitimately spans several
      validity rows sharing one id (new rows draw from the sequence, while new
      validity periods reuse their id).
    * Calls `versioning_trigger/0` first if you haven't already, the function
      must exist before the trigger is attached. The helper does *not* call it
      for you to keep installations explicit, so a typical host calls
      `versioning_trigger()` once in an early migration, then
      `bitemporal_table/2` any number of times.

  Reversible: `down` drops the table (cascading to the history twin via the
  `_history`-named convention, drop them in pairs).
  """
  @spec bitemporal_table(atom(), String.t()) :: :ok
  def bitemporal_table(table, data_columns)
      when is_atom(table) and is_binary(data_columns) do
    for sql <- bitemporal_table_sql(table, data_columns), do: Ecto.Migration.execute(sql)
    :ok
  end

  @doc false
  # Returns the SQL statements that `bitemporal_table/2` would execute, in
  # order. Exposed for testing and for host migrations that want to combine or
  # tweak the DDL.
  @spec bitemporal_table_sql(atom(), String.t()) :: [String.t()]
  def bitemporal_table_sql(table, data_columns)
      when is_atom(table) and is_binary(data_columns) do
    name = Atom.to_string(table)
    seq = "#{name}_id_seq"
    history = "#{name}_history"

    [
      "CREATE SEQUENCE #{seq}",
      """
      CREATE TABLE #{name} (
        id BIGINT NOT NULL DEFAULT nextval('#{seq}'),
        #{data_columns},
        valid_time tstzrange NOT NULL DEFAULT tstzrange(now(), NULL),
        system_time tstzrange NOT NULL DEFAULT tstzrange(now(), NULL),
        PRIMARY KEY (id, valid_time WITHOUT OVERLAPS)
      )
      """,
      "ALTER SEQUENCE #{seq} OWNED BY #{name}.id",
      """
      CREATE TABLE #{history} (LIKE #{name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
      """,
      """
      CREATE INDEX #{history}_id_system_time_idx
        ON #{history} USING gist (id, system_time)
      """,
      """
      CREATE TRIGGER #{name}_versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON #{name}
        FOR EACH ROW EXECUTE FUNCTION temporal_versioning()
      """
    ]
  end

  @doc """
  Creates a vertex table for a property graph (PG19 SQL/PGQ).

  Vertex tables are identity registries, one row per logical entity, kept in
  lockstep with a domain table via a host-supplied sync trigger. The helper
  emits the basic shape. The host writes their own `AFTER INSERT` sync trigger
  (the rule for "what counts as a new vertex" is domain-specific).

  Options:

    * `:references`, `{column, parent_table}` to make `id` a foreign key to a
      domain table with `ON DELETE CASCADE`. Omit for vertex tables that own
      their identity (no parent domain table, like a `route_vertices`
      registry).
    * `:extra_columns`, SQL fragment for additional columns (e.g.
      `route_vertices.code`).

  Examples:

      vertex_table(:hub_vertices, references: {:id, :hubs})
      vertex_table(:asset_vertices)  # no FK, parent's PK is temporal, not referenceable
      vertex_table(:route_vertices, extra_columns: "code text NOT NULL UNIQUE")
  """
  @spec vertex_table(atom(), keyword()) :: :ok
  def vertex_table(table, opts \\ []) when is_atom(table) do
    name = Atom.to_string(table)

    id_clause =
      case Keyword.fetch(opts, :references) do
        {:ok, {col, parent}} ->
          parent_col = Atom.to_string(col)
          parent_table = Atom.to_string(parent)
          "id BIGINT PRIMARY KEY REFERENCES #{parent_table} (#{parent_col}) ON DELETE CASCADE"

        :error ->
          "id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"
      end

    extra = Keyword.get(opts, :extra_columns, "")

    columns =
      case extra do
        "" -> id_clause
        _ -> "#{id_clause}, #{extra}"
      end

    Ecto.Migration.execute("""
    CREATE TABLE #{name} (#{columns})
    """)

    :ok
  end

  @doc """
  Creates a bi-temporal edge table for a property graph.

  `source` and `dest` are `{column, vertex_table}` tuples identifying the two
  endpoints. The helper emits:

      CREATE TABLE <name> (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        <src_col> BIGINT NOT NULL REFERENCES <src_table> (id),
        <dst_col> BIGINT NOT NULL REFERENCES <dst_table> (id) ON DELETE CASCADE,
        valid_time  tstzrange NOT NULL DEFAULT tstzrange(now(), NULL),
        system_time tstzrange NOT NULL DEFAULT tstzrange(now(), NULL),
        <optional CHECK and unique_extra>,
        UNIQUE (<src_col>, <dst_col>, valid_time WITHOUT OVERLAPS)
      )

  plus the standard `_history` twin, GiST index, and versioning trigger.

  Options:

    * `:check`, SQL fragment for a `CHECK` constraint (e.g.
      `"from_id <> to_id"` for a self-loop-banning edge).
    * `:cascade_source?`, when true, adds `ON DELETE CASCADE` to the source
      FK too (default: only the destination cascades).
  """
  @spec edge_table(atom(), {atom(), atom()}, {atom(), atom()}, keyword()) :: :ok
  def edge_table(table, {src_col, src_table}, {dst_col, dst_table}, opts \\ [])
      when is_atom(table) do
    for sql <- edge_table_sql(table, {src_col, src_table}, {dst_col, dst_table}, opts),
        do: Ecto.Migration.execute(sql)

    :ok
  end

  @doc false
  @spec edge_table_sql(atom(), {atom(), atom()}, {atom(), atom()}, keyword()) :: [String.t()]
  def edge_table_sql(table, {src_col, src_table}, {dst_col, dst_table}, opts \\ [])
      when is_atom(table) do
    name = Atom.to_string(table)
    src = Atom.to_string(src_col)
    src_t = Atom.to_string(src_table)
    dst = Atom.to_string(dst_col)
    dst_t = Atom.to_string(dst_table)
    history = "#{name}_history"

    src_fk =
      if Keyword.get(opts, :cascade_source?, false) do
        "REFERENCES #{src_t} (id) ON DELETE CASCADE"
      else
        "REFERENCES #{src_t} (id)"
      end

    check_clause =
      case Keyword.fetch(opts, :check) do
        {:ok, sql} -> ", CHECK (#{sql})"
        :error -> ""
      end

    [
      """
      CREATE TABLE #{name} (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        #{src} BIGINT NOT NULL #{src_fk},
        #{dst} BIGINT NOT NULL REFERENCES #{dst_t} (id) ON DELETE CASCADE,
        valid_time tstzrange NOT NULL DEFAULT tstzrange(now(), NULL),
        system_time tstzrange NOT NULL DEFAULT tstzrange(now(), NULL)#{check_clause},
        UNIQUE (#{src}, #{dst}, valid_time WITHOUT OVERLAPS)
      )
      """,
      """
      CREATE TABLE #{history} (LIKE #{name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
      """,
      """
      CREATE INDEX #{history}_id_system_time_idx
        ON #{history} USING gist (id, system_time)
      """,
      """
      CREATE TRIGGER #{name}_versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON #{name}
        FOR EACH ROW EXECUTE FUNCTION temporal_versioning()
      """
    ]
  end

  @doc """
  Creates a pgmq durable queue (safe to call before
  `Ergon.Pgmq` ships, it just installs the queue tables).

  Reversible in `change/0`, `down` drops the queue via `pgmq.drop_queue`.
  """
  @spec pgmq_queue(atom() | String.t()) :: :ok
  def pgmq_queue(name) when is_atom(name) or is_binary(name) do
    queue = validate_identifier!(name, "queue")

    Ecto.Migration.execute(
      "SELECT pgmq.create('#{queue}')",
      "SELECT pgmq.drop_queue('#{queue}')"
    )

    :ok
  end

  @doc """
  Installs an `AFTER INSERT` trigger on `pgmq.q_<queue>` that fires
  `pg_notify(channel, '')` on every enqueued message, the emitting half of
  `Ergon.Pgmq.Producer`'s `:notify_channel` LISTEN fast-path. Without this,
  nothing ever calls `pg_notify` on that channel and the producer silently
  falls back to polling on `:poll_interval` alone. The two only work together
  once both sides are wired up.

  `channel` defaults to `pgmq_<queue>`, matching the convention documented on
  `Ergon.Pgmq.Producer`. No payload is sent (`pg_notify(channel, '')`), by
  design, listeners are expected to poll pgmq themselves rather than trust
  the notification payload, matching pgmq's own at-least-once contract (a
  dropped `NOTIFY` only costs latency, never a missed message). Firing
  `pg_notify` from inside the trigger is safe with respect to rolled-back
  work: Postgres only delivers a `NOTIFY` once its transaction commits, so an
  aborted `pgmq.send` never wakes a listener for a message that doesn't
  exist.

  Triggers on the table Postgres uses for the queue rather than requiring
  every caller of `pgmq.send` to remember to notify, any inserter (raw SQL,
  another app, a future pgmq version) gets the wake-up for free.

  Reversible in `change/0` (the function and trigger have no meaningful
  partial `down` beyond `DROP`, same rationale as the other DDL helpers here).
  """
  @spec pgmq_notify_trigger(atom() | String.t(), keyword()) :: :ok
  def pgmq_notify_trigger(queue, opts \\ []) when is_atom(queue) or is_binary(queue) do
    for sql <- pgmq_notify_trigger_sql(queue, opts), do: Ecto.Migration.execute(sql)
    :ok
  end

  @doc false
  # Returns the SQL statements `pgmq_notify_trigger/2` would execute, in
  # order. Exposed for testing, the migration-execute form can only run
  # inside a live `Ecto.Migration.Runner`, same reason `bitemporal_table_sql/2`
  # and friends exist.
  @spec pgmq_notify_trigger_sql(atom() | String.t(), keyword()) :: [String.t()]
  def pgmq_notify_trigger_sql(queue, opts \\ []) when is_atom(queue) or is_binary(queue) do
    name = validate_identifier!(queue, "queue")
    channel = opts |> Keyword.get(:channel, "pgmq_#{name}") |> validate_identifier!("channel")
    fn_name = "pgmq_notify_#{name}"

    [
      """
      CREATE OR REPLACE FUNCTION #{fn_name}() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM pg_notify('#{channel}', '');
        RETURN NEW;
      END
      $$
      """,
      """
      CREATE TRIGGER #{fn_name}_trigger
        AFTER INSERT ON pgmq.q_#{name}
        FOR EACH ROW EXECUTE FUNCTION #{fn_name}()
      """
    ]
  end

  @doc """
  Installs the wake-up trigger on `ergon.jobs` for `Ergon.JobNotifier`.

  Emits an `AFTER INSERT OR UPDATE` trigger firing
  `pg_notify('#{Ergon.JobNotifier.channel()}', NEW.queue)` **only** when the
  row is immediately runnable, `available`, due (`scheduled_at <= now()`), and
  live (`upper(valid_period) = 'infinity'`). The payload is the queue name
  alone, never job payload or `tenant` (`NOTIFY` bypasses RLS, so the channel
  is visible to any session on the database, the queue name is the only thing
  exposed). `Ergon.JobNotifier` routes on that payload to wake the workers
  draining that queue.

  This is the native-`ergon.jobs` mirror of `pgmq_notify_trigger/2`. It is
  shipped by ergon's own initial migration (`ergon.jobs` is ergon's table, not
  a host concern like pgmq queues), and exposed here for parity and so a host
  that rebuilds the schema can reinstall it.

  Why the guard matters:

    * **Only runnable rows wake.** A checkout (`available → executing`) sets
      `NEW.state = 'executing'`, so it never fires. A backoff retry scheduled
      in the future has `scheduled_at > now()`, so it does not wake anyone
      early, the fallback poll picks it up when due.
    * **Commit semantics come free.** Postgres delivers a `NOTIFY` only once
      its transaction commits, so a rolled-back enqueue never wakes a worker,
      and a multi-row batch enqueue on one queue coalesces to a single wake
      (Postgres dedups `(channel, payload)` within a transaction).

  Idempotent (`CREATE OR REPLACE FUNCTION`), the trigger is created plain, so
  reinstalling against a schema that already has it needs a `DROP TRIGGER`
  first (the initial migration installs it exactly once).
  """
  @spec job_notify_trigger() :: :ok
  def job_notify_trigger do
    for sql <- job_notify_trigger_sql(), do: Ecto.Migration.execute(sql)
    :ok
  end

  @doc false
  # Returns the SQL statements `job_notify_trigger/0` would execute, in order
  # (function DDL, then the guarded trigger). Exposed for testing, same reason
  # as pgmq_notify_trigger_sql/2.
  @spec job_notify_trigger_sql() :: [String.t()]
  def job_notify_trigger_sql do
    channel = Ergon.JobNotifier.channel()

    [
      """
      CREATE OR REPLACE FUNCTION ergon.job_notify() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM pg_notify('#{channel}', NEW.queue);
        RETURN NEW;
      END
      $$
      """,
      """
      CREATE TRIGGER jobs_notify_trigger
        AFTER INSERT OR UPDATE ON ergon.jobs
        FOR EACH ROW
        WHEN (
          NEW.state = 'available'
          AND NEW.scheduled_at <= now()
          AND upper(NEW.valid_period) = 'infinity'
        )
        EXECUTE FUNCTION ergon.job_notify()
      """
    ]
  end

  # Postgres unquoted identifiers must match [a-z_][a-z0-9_]*. Queue and
  # channel names get interpolated into DDL (table/function/trigger names,
  # or a NOTIFY channel) where parameterised queries can't reach, so they're
  # validated up front instead, both to close off SQL injection via a
  # crafted name and to fail fast with a clear error at migration time
  # rather than a confusing failure from pgmq or a broken channel name later.
  defp validate_identifier!(value, kind) do
    name = normalize_name(value)

    unless Regex.match?(~r/^[a-z_][a-z0-9_]*$/, name) do
      raise ArgumentError, "invalid #{kind} name: #{inspect(name)}"
    end

    name
  end

  @doc """
  Installs the per-table `auto_manage_partitions_<table>(months_ahead)`
  function for a RANGE-partitioned table with monthly partitions named
  `<table>_YYYYMM`.

  The function creates any missing monthly partitions from the current month
  through `months_ahead` months out. It is the single implementation shared
  by all callers, the initial migration (calling it once with the default),
  the weekly `pg_cron` job (scheduled here via `Ergon.Cron.schedule/3`), and
  `Ergon.PartitionBootCheck` (started in the host's supervision tree).

  Postgres function names can't be parameterised at DDL time, so the table
  name is baked into the identifier, one function per partitioned table.
  The weekly cron schedule is named `partition-lifecycle-<table>`. Calling
  `partitioned_table/2` again with the same table name updates the schedule
  in place (idempotent via `cron.schedule`'s upsert).

  The parent table itself (`CREATE TABLE ... PARTITION BY RANGE (<col>)`) is
  not created here, the host owns the schema. The `partition_column`
  argument is documentation-only for now (the function body bakes in the
  monthly RANGE assumption). Future work may emit the parent DDL too.
  """
  @spec partitioned_table(atom() | String.t(), atom() | String.t()) :: :ok
  def partitioned_table(table, partition_column)
      when (is_atom(table) or is_binary(table)) and
             (is_atom(partition_column) or is_binary(partition_column)) do
    for sql <- partitioned_table_sql(table, partition_column), do: Ecto.Migration.execute(sql)

    name = normalize_name(table)
    fn_name = "auto_manage_partitions_#{name}"

    # Weekly lifecycle job, runs the per-table function to keep the
    # partition horizon ahead of ingestion. No-op where pg_cron isn't
    # installed (test DB, host clusters without pg_cron).
    Ergon.Cron.schedule(
      "partition-lifecycle-#{name}",
      "@weekly",
      "SELECT #{fn_name}()"
    )

    :ok
  end

  @doc false
  # Returns the SQL statements that `partitioned_table/2` would execute, in
  # order (function DDL, then the initial-horizon call). Exposed for testing
  # and for host migrations that want to combine or tweak the DDL, the cron
  # schedule is NOT included here (it's a migration-only side-effect).
  @spec partitioned_table_sql(atom() | String.t(), atom() | String.t()) :: [String.t()]
  def partitioned_table_sql(table, _partition_column)
      when is_atom(table) or is_binary(table) do
    name = normalize_name(table)
    fn_name = "auto_manage_partitions_#{name}"

    [
      """
      CREATE OR REPLACE FUNCTION #{fn_name}(months_ahead int DEFAULT 2)
      RETURNS int
      LANGUAGE plpgsql AS $$
      DECLARE
        month_start date;
        partition_name text;
        created int := 0;
      BEGIN
        FOR i IN 0..months_ahead LOOP
          month_start := (date_trunc('month', now()) + make_interval(months => i))::date;
          partition_name := '#{name}_' || to_char(month_start, 'YYYYMM');

          IF to_regclass(partition_name) IS NULL THEN
            EXECUTE format(
              'CREATE TABLE %I PARTITION OF #{name} FOR VALUES FROM (%L) TO (%L)',
              partition_name, month_start, month_start + interval '1 month'
            );
            created := created + 1;
          END IF;
        END LOOP;

        RETURN created;
      END
      $$
      """,
      "SELECT #{fn_name}()"
    ]
  end

  defp normalize_name(v) when is_atom(v), do: Atom.to_string(v)
  defp normalize_name(v) when is_binary(v), do: v
end
