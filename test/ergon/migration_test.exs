defmodule Ergon.MigrationTest do
  # Exercises Ergon.Migration helpers.
  # Two layers:
  #
  #   1. "init migration state", verify post-migration schema state that the
  #      init migration produces via `extensions/0` and `versioning_trigger/0`.
  #      These run against the already-migrated test DB, no DDL issued here.
  #
  #   2. "bitemporal_table_sql/2" / "edge_table_sql/2", exercise the
  #      statement-list helpers end-to-end inside the sandbox transaction.
  #      Postgres DDL is transactional, so `DROP`-free cleanup via rollback
  #      works. The `temporal_versioning()` function installed by the init
  #      migration is visible to these tests.
  use Ergon.Case, async: true

  @moduletag :integration

  alias Ergon.Repo

  #      Postgres extname is text, not ident, parameterise to dodge SQL injection
  # lint while keeping the query readable.
  @ext_installed "SELECT 1 FROM pg_extension WHERE extname = $1"

  describe "init migration state (extensions/0 + versioning_trigger/0)" do
    test "btree_gist is installed" do
      assert {:ok, %Postgrex.Result{rows: [[1]]}} = Repo.query(@ext_installed, ["btree_gist"])
    end

    test "pgmq is installed" do
      assert {:ok, %Postgrex.Result{rows: [[1]]}} = Repo.query(@ext_installed, ["pgmq"])
    end

    test "pg_cron is NOT installed in the test database" do
      # `cron.database_name` is cluster-wide and points at the dev DB (`ergon`),
      # so the `current_database() = current_setting('cron.database_name')`
      # guard in extensions/0 skips pg_cron here (`ergon_test`). This is the
      # contract that lets the same migration run cleanly in dev and test.
      assert {:ok, %Postgrex.Result{rows: []}} = Repo.query(@ext_installed, ["pg_cron"])
    end

    test "temporal_versioning() function is installed" do
      assert {:ok, %Postgrex.Result{rows: [[1]]}} =
               Repo.query("SELECT 1 FROM pg_proc WHERE proname = 'temporal_versioning'")
    end
  end

  describe "job_notify_trigger_sql/0" do
    test "returns two statements: the notify function, then the guarded trigger" do
      sqls = Ergon.Migration.job_notify_trigger_sql()

      assert length(sqls) == 2

      # Payload is the queue name only (never job data or tenant, NOTIFY
      # bypasses RLS).
      assert Enum.at(sqls, 0) =~ "CREATE OR REPLACE FUNCTION ergon.job_notify()"
      assert Enum.at(sqls, 0) =~ "pg_notify('#{Ergon.JobNotifier.channel()}', NEW.queue)"

      trigger = Enum.at(sqls, 1)
      assert trigger =~ "CREATE TRIGGER jobs_notify_trigger"
      assert trigger =~ "AFTER INSERT OR UPDATE ON ergon.jobs"
      assert trigger =~ "NEW.state = 'available'"
      assert trigger =~ "NEW.scheduled_at <= now()"
      assert trigger =~ "upper(NEW.valid_period) = 'infinity'"
      assert trigger =~ "EXECUTE FUNCTION ergon.job_notify()"
    end
  end

  describe "bitemporal_table_sql/2" do
    test "returns six statements in the expected order" do
      sqls = Ergon.Migration.bitemporal_table_sql(:widgets, "name text NOT NULL")

      assert length(sqls) == 6

      # Sequence first so the table's DEFAULT nextval resolves at CREATE time.
      assert Enum.at(sqls, 0) =~ ~r/^CREATE SEQUENCE widgets_id_seq\b/

      # Main table with temporal PK + both range columns.
      table = Enum.at(sqls, 1)
      assert table =~ "CREATE TABLE widgets"
      assert table =~ "name text NOT NULL"
      assert table =~ "valid_time tstzrange"
      assert table =~ "system_time tstzrange"
      assert table =~ "PRIMARY KEY (id, valid_time WITHOUT OVERLAPS)"

      # History twin via LIKE INCLUDING CONSTRAINTS (so the GiST PK shape is
      # inherited without re-declaring it).
      assert Enum.at(sqls, 3) =~ "CREATE TABLE widgets_history (LIKE widgets"

      # GiST index on the history twin's system_time for time-travel queries.
      assert Enum.at(sqls, 4) =~ "USING gist (id, system_time)"

      # Trigger attachment is last, temporal_versioning() must already exist.
      assert Enum.at(sqls, 5) =~ "CREATE TRIGGER widgets_versioning_trigger"
      assert Enum.at(sqls, 5) =~ "EXECUTE FUNCTION temporal_versioning()"
    end

    @tag :integration
    test "executing the statements yields a working bi-temporal table" do
      sqls = Ergon.Migration.bitemporal_table_sql(:bt_widget, "name text NOT NULL")
      for sql <- sqls, do: {:ok, _} = Repo.query(sql)

      # Sanity: the table, its history twin, and the trigger exist.
      assert {:ok, %Postgrex.Result{rows: [["bt_widget"]]}} =
               Repo.query("SELECT to_regclass('bt_widget')::text")

      assert {:ok, %Postgrex.Result{rows: [["bt_widget_history"]]}} =
               Repo.query("SELECT to_regclass('bt_widget_history')::text")

      assert {:ok, %Postgrex.Result{rows: [[1]]}} =
               Repo.query("SELECT 1 FROM pg_trigger WHERE tgname = 'bt_widget_versioning_trigger'")

      # Insert, then mutate a non-key column. The versioning trigger should
      # archive the OLD row (with its closed system_time) into history and
      # leave the NEW row live in the main table.
      {:ok, _} =
        Repo.query("INSERT INTO bt_widget (name) VALUES ($1)", ["alpha"])

      {:ok, _} =
        Repo.query("UPDATE bt_widget SET name = $1 WHERE name = $2", ["beta", "alpha"])

      assert {:ok, %Postgrex.Result{rows: [[1]]}} =
               Repo.query("SELECT count(*) FROM bt_widget_history")

      assert {:ok, %Postgrex.Result{rows: [["beta"]]}} =
               Repo.query("SELECT name FROM bt_widget")

      assert {:ok, %Postgrex.Result{rows: [["alpha"]]}} =
               Repo.query("SELECT name FROM bt_widget_history")
    end
  end

  describe "edge_table_sql/2" do
    test "returns four statements in the expected order" do
      sqls =
        Ergon.Migration.edge_table_sql(
          :contains,
          {:parent_id, :parents},
          {:child_id, :children},
          check: "parent_id <> child_id"
        )

      assert length(sqls) == 4

      table = Enum.at(sqls, 0)
      assert table =~ "CREATE TABLE contains"
      assert table =~ "parent_id BIGINT NOT NULL REFERENCES parents (id)"
      assert table =~ "child_id BIGINT NOT NULL REFERENCES children (id) ON DELETE CASCADE"
      assert table =~ "CHECK (parent_id <> child_id)"
      assert table =~ "UNIQUE (parent_id, child_id, valid_time WITHOUT OVERLAPS)"

      assert Enum.at(sqls, 1) =~ "CREATE TABLE contains_history (LIKE contains"
      assert Enum.at(sqls, 2) =~ "USING gist (id, system_time)"
      assert Enum.at(sqls, 3) =~ "EXECUTE FUNCTION temporal_versioning()"
    end

    test "cascade_source? option adds ON DELETE CASCADE to the source FK" do
      sqls =
        Ergon.Migration.edge_table_sql(
          :contains,
          {:parent_id, :parents},
          {:child_id, :children},
          cascade_source?: true
        )

      # Both FKs cascade when cascade_source? is set. Without it, only the
      # destination cascades (the default).
      table = Enum.at(sqls, 0)
      assert table =~ "parent_id BIGINT NOT NULL REFERENCES parents (id) ON DELETE CASCADE"
      assert table =~ "child_id BIGINT NOT NULL REFERENCES children (id) ON DELETE CASCADE"
    end

    @tag :integration
    test "executing the statements yields a temporal edge table" do
      # The edge helper emits FKs to vertex tables, set those up first so the
      # constraints can resolve.
      {:ok, _} = Repo.query("CREATE TABLE etc_parents (id BIGINT PRIMARY KEY)")
      {:ok, _} = Repo.query("CREATE TABLE etc_children (id BIGINT PRIMARY KEY)")

      sqls =
        Ergon.Migration.edge_table_sql(
          :etc_contains,
          {:parent_id, :etc_parents},
          {:child_id, :etc_children}
        )

      for sql <- sqls, do: {:ok, _} = Repo.query(sql)

      assert {:ok, %Postgrex.Result{rows: [["etc_contains"]]}} =
               Repo.query("SELECT to_regclass('etc_contains')::text")

      assert {:ok, %Postgrex.Result{rows: [["etc_contains_history"]]}} =
               Repo.query("SELECT to_regclass('etc_contains_history')::text")
    end
  end

  describe "pgmq_notify_trigger_sql/2" do
    test "returns two statements: the notify function, then the trigger" do
      sqls = Ergon.Migration.pgmq_notify_trigger_sql(:asset_events)

      assert length(sqls) == 2
      assert Enum.at(sqls, 0) =~ "CREATE OR REPLACE FUNCTION pgmq_notify_asset_events()"
      assert Enum.at(sqls, 0) =~ "pg_notify('pgmq_asset_events', '')"
      assert Enum.at(sqls, 1) =~ "CREATE TRIGGER pgmq_notify_asset_events_trigger"
      assert Enum.at(sqls, 1) =~ "AFTER INSERT ON pgmq.q_asset_events"
      assert Enum.at(sqls, 1) =~ "EXECUTE FUNCTION pgmq_notify_asset_events()"
    end

    test "a :channel override replaces the pgmq_<queue> default" do
      sqls = Ergon.Migration.pgmq_notify_trigger_sql(:asset_events, channel: "custom_chan")

      assert Enum.at(sqls, 0) =~ "pg_notify('custom_chan', '')"
    end

    test "rejects a queue name with characters outside [a-z0-9_]" do
      assert_raise ArgumentError, ~r/invalid queue name/, fn ->
        Ergon.Migration.pgmq_notify_trigger_sql("bad; drop table x --")
      end
    end

    test "rejects a channel override with unsafe characters" do
      assert_raise ArgumentError, ~r/invalid channel name/, fn ->
        Ergon.Migration.pgmq_notify_trigger_sql(:asset_events, channel: "bad-channel!")
      end
    end

    @tag :integration
    test "executing the statements installs a working notify trigger on the queue table" do
      queue = "notify_test_#{System.unique_integer([:positive])}"
      {:ok, _} = Repo.query("SELECT pgmq.create($1)", [queue])

      for sql <- Ergon.Migration.pgmq_notify_trigger_sql(queue),
          do: {:ok, _} = Repo.query(sql)

      assert {:ok, %Postgrex.Result{rows: [[1]]}} =
               Repo.query(
                 "SELECT 1 FROM pg_trigger WHERE tgname = $1",
                 ["pgmq_notify_#{queue}_trigger"]
               )

      assert {:ok, %Postgrex.Result{rows: [[funcdef]]}} =
               Repo.query("SELECT pg_get_functiondef('pgmq_notify_#{queue}'::regproc)")

      assert funcdef =~ "pg_notify('pgmq_#{queue}', '')"

      # NOTIFY only delivers on commit, the sandbox transaction never
      # commits, so we can't observe an actual notification here (see
      # Ergon.Pgmq.ProducerTest's own note on the same limitation). Enqueuing
      # without error is as far as this test can verify the trigger fires
      # without raising.
      assert {:ok, %Postgrex.Result{rows: [[_msg_id]]}} =
               Repo.query("SELECT pgmq.send($1, $2)", [queue, %{"probe" => true}])
    end
  end
end
