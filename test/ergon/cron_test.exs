defmodule Ergon.CronTest do
  # Exercises the guarded pg_cron wrappers.
  #
  # Three layers:
  #
  #   1. Pure SQL generation, the generated string contains the existence
  #      guard and the cron.schedule/unschedule call, with arguments safely
  #      single-quoted. No database needed.
  #
  #   2. No-op path (against the test DB, which lacks pg_cron by design,'
  #      Phase 1's extensions/0 guards it via `current_database() =
  #      current_setting('cron.database_name')`). The DO block's IF
  #      short-circuits, cron.schedule is never invoked.
  #
  #   3. Active path (against the dev DB, which DOES have pg_cron via the
  #      devenv flake). Tagged `:cron` at the describe level, excluded by
  #      default. Running it mutates `cron.job` in dev, each test uses a
  #      unique job name derived from the test name and cleans up via
  #      `on_exit/1`.
  #
  # `Ergon.Case` is used for the sandbox it sets up for layer 2. Layer 3
  # bypasses it entirely with a fresh Postgrex connection per query (the
  # dev DB is a different database from the sandbox's `ergon_test`).
  use Ergon.Case, async: true

  alias Ergon.Cron

  describe "schedule_sql/3" do
    test "wraps cron.schedule in the pg_cron existence guard" do
      sql = Cron.schedule_sql("hourly", "0 * * * *", "SELECT 1")

      assert sql =~ "IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron')"
      assert sql =~ "cron.schedule("
      assert sql =~ "'hourly'"
      assert sql =~ "'0 * * * *'"
      assert sql =~ "'SELECT 1'"
    end

    test "escapes single quotes in arguments" do
      sql = Cron.schedule_sql("with 'quote'", "* * * * *", "SELECT 'hello'")

      # SQL single-quoted literals double their internal quotes.
      assert sql =~ "'with ''quote'''"
      assert sql =~ "'SELECT ''hello'''"
    end
  end

  describe "unschedule_sql/1" do
    test "wraps cron.unschedule in the pg_cron existence guard" do
      sql = Cron.unschedule_sql("hourly")

      assert sql =~ "IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron')"
      assert sql =~ "cron.unschedule("
      assert sql =~ "'hourly'"
    end

    test "guards on job existence so unscheduling a missing job is a no-op" do
      sql = Cron.unschedule_sql("hourly")

      assert sql =~
               "IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'hourly') THEN"
    end

    test "escapes single quotes in the job name" do
      sql = Cron.unschedule_sql("with 'quote'")
      assert sql =~ "'with ''quote'''"
    end
  end

  describe "no-op without pg_cron (test DB)" do
    @describetag :integration

    # The test DB intentionally does NOT have pg_cron installed. Running the
    #      generated DO block here must succeed (the IF guard short-circuits), it
    # is the contract that lets the same migration run in dev and test.

    test "schedule_sql is a no-op against the test DB" do
      assert {:ok, _} =
               Ergon.Repo.query(Cron.schedule_sql("ergon-cron-noop", "* * * * *", "SELECT 1"), [])
    end

    test "unschedule_sql is a no-op against the test DB" do
      assert {:ok, _} = Ergon.Repo.query(Cron.unschedule_sql("ergon-cron-noop"), [])
    end
  end

  describe "active path against dev DB (pg_cron installed)" do
    # Excluded by default (see test_helper.exs), run with `mix test --include cron`.
    # Each test uses a unique job name derived from the test's own name, so
    # concurrent cron-active tests don't contend on `cron.job` rows.
    @describetag :cron
    @describetag :integration

    setup context do
      job_name = "ergon-cron-test-#{context.test}"

      on_exit(fn ->
        # Best-effort cleanup, unschedule_sql is idempotent (guards on job
        # existence), so a missing row is a silent no-op.
        dev_query!(Cron.unschedule_sql(job_name))
      end)

      {:ok, job_name: job_name}
    end

    @tag :cron
    test "schedule/3 twice with same name leaves exactly one row in cron.job", %{job_name: n} do
      dev_query!(Cron.schedule_sql(n, "* * * * *", "SELECT 1"))
      dev_query!(Cron.schedule_sql(n, "* * * * *", "SELECT 1"))

      %Postgrex.Result{rows: [[count]]} =
        dev_query!("SELECT count(*) FROM cron.job WHERE jobname = $1", [n])

      assert count == 1
    end

    @tag :cron
    test "schedule/3 with new args updates the existing row in place", %{job_name: n} do
      dev_query!(Cron.schedule_sql(n, "* * * * *", "SELECT 1"))
      dev_query!(Cron.schedule_sql(n, "*/5 * * * *", "SELECT 2"))

      %Postgrex.Result{rows: [[count, schedule, command]]} =
        dev_query!(
          "SELECT count(*), schedule, command FROM cron.job WHERE jobname = $1 GROUP BY schedule, command",
          [n]
        )

      assert count == 1
      assert schedule == "*/5 * * * *"
      assert command == "SELECT 2"
    end

    @tag :cron
    test "unschedule/1 removes the job", %{job_name: n} do
      dev_query!(Cron.schedule_sql(n, "* * * * *", "SELECT 1"))

      %Postgrex.Result{rows: [[1]]} =
        dev_query!("SELECT count(*) FROM cron.job WHERE jobname = $1", [n])

      dev_query!(Cron.unschedule_sql(n))

      %Postgrex.Result{rows: [[0]]} =
        dev_query!("SELECT count(*) FROM cron.job WHERE jobname = $1", [n])
    end

    @tag :cron
    test "unschedule/1 is a no-op when the job does not exist", %{job_name: n} do
      # Idempotent contract, the existence guard makes this safe.
      assert {:ok, _} = dev_query(Cron.unschedule_sql(n))
    end

    # Each call opens its own short-lived Postgrex connection so the test
    # process and the on_exit callback (which runs in a different process)
    # can both issue queries without ownership races.
    defp dev_opts do
      [
        hostname: System.get_env("PGHOST", "127.0.0.1"),
        port: String.to_integer(System.get_env("PGPORT", "5432")),
        username: System.get_env("PGUSER", "ergon"),
        password: System.get_env("PGPASSWORD", "ergon"),
        # The DEV database, pg_cron is installed here, not in *_test.
        database: System.get_env("PGDATABASE", "ergon")
      ]
    end

    defp dev_query(sql, params \\ []) do
      {:ok, conn} = Postgrex.start_link(dev_opts())

      try do
        Postgrex.query(conn, sql, params)
      after
        GenServer.stop(conn, :normal)
      end
    end

    defp dev_query!(sql, params \\ []) do
      {:ok, conn} = Postgrex.start_link(dev_opts())

      try do
        Postgrex.query!(conn, sql, params)
      after
        GenServer.stop(conn, :normal)
      end
    end
  end
end
