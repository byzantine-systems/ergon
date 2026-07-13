defmodule Ergon.Cron do
  @moduledoc """
  Migration helpers for scheduling SQL with pg_cron.

  pg_cron can be `CREATE EXTENSION`'d in exactly one database per cluster
  (set by `cron.database_name` in `postgresql.conf`). The dev database has it,
  the test database does not, and host clusters vary. Every helper here is a
  guarded no-op when pg_cron is absent, so the same migration runs cleanly
  in dev (cron active) and test (cron absent). A reusable helper so migrations
  stop hand-rolling the `DO $$ … $$` guard.

  Idempotent via `cron.schedule`'s upsert-by-name semantics (pg_cron 1.6+,
  pinned in `flake.nix`): calling `schedule/3` twice with the same `name`
  updates the schedule/command in place rather than creating a duplicate,'
  exactly the contract a migration needs to be re-runnable.

  Both helpers are `execute/1`-style, call them from a migration's `up/0`:

      def up do
        Ergon.Cron.schedule("hourly-report", "0 * * * *", "SELECT hourly_report()")
      end

      def down do
        Ergon.Cron.unschedule("hourly-report")
      end
  """

  @doc """
  Schedule `sql` to run on the cron `spec` (standard 5-field Vixie-cron
  syntax), named `name` for later lookup/unscheduling. No-op when pg_cron is
  not installed.
  """
  @spec schedule(String.t(), String.t(), String.t()) :: :ok
  def schedule(name, spec, sql)
      when is_binary(name) and is_binary(spec) and is_binary(sql) do
    Ecto.Migration.execute(schedule_sql(name, spec, sql))
    :ok
  end

  @doc """
  Unschedule the cron job named `name`. No-op when pg_cron is not installed.
  """
  @spec unschedule(String.t()) :: :ok
  def unschedule(name) when is_binary(name) do
    Ecto.Migration.execute(unschedule_sql(name))
    :ok
  end

  @doc false
  # Returns the SQL string that `schedule/3` executes. Exposed for testing,'
  # `Ecto.Migration.execute/1` only works inside an Ecto migration callback,
  # so tests run the string directly against a Postgrex connection.
  @spec schedule_sql(String.t(), String.t(), String.t()) :: String.t()
  def schedule_sql(name, spec, sql)
      when is_binary(name) and is_binary(spec) and is_binary(sql) do
    """
    DO $$
    BEGIN
      IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule(#{lit(name)}, #{lit(spec)}, #{lit(sql)});
      END IF;
    END
    $$
    """
  end

  @doc false
  @spec unschedule_sql(String.t()) :: String.t()
  def unschedule_sql(name) when is_binary(name) do
    """
    DO $$
    BEGIN
      IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = #{lit(name)}) THEN
          PERFORM cron.unschedule(#{lit(name)});
        END IF;
      END IF;
    END
    $$
    """
  end

  # SQL single-quoted string literal: double internal single quotes.
  # Migration helpers take developer-authored input, not user input, but the
  # SQL passed to cron.schedule frequently contains quoted literals
  # (e.g. `SELECT foo('bar')`), so proper escaping is required.
  defp lit(s) when is_binary(s) do
    "'" <> String.replace(s, "'", "''") <> "'"
  end
end
