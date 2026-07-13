defmodule Ergon.PartitionBootCheckTest do
  # Exercises the generic partition boot check.
  #
  # Not async, PartitionBootCheck runs queries from its own process during
  # init/1, so the test must own the sandbox connection in shared mode (the
  # default for async: false in Ergon.Case).
  use Ergon.Case, async: false

  @moduletag :integration

  alias Ergon.{PartitionBootCheck, Repo}

  # Each test gets its own partitioned table so concurrent (or serial) tests
  # don't contend on the same monthly partitions.
  setup do
    table = "pbc_test_#{System.unique_integer([:positive])}"

    # Parent table, partitioned by RANGE on recorded_at. The parent must
    # exist before partitioned_table_sql/2 is called (it installs the
    # auto_manage_partitions_<table>() function that creates partitions
    # against this parent).
    {:ok, _} =
      Repo.query("""
      CREATE TABLE #{table} (
        id bigint NOT NULL,
        recorded_at timestamptz NOT NULL
      ) PARTITION BY RANGE (recorded_at)
      """)

    # Install the per-table manage function and create the initial horizon.
    # partitioned_table_sql/2 is the test-friendly variant of the migration
    # helper (no Ecto.Migration.execute/1 required).
    for sql <- Ergon.Migration.partitioned_table_sql(table, :recorded_at) do
      {:ok, _} = Repo.query(sql)
    end

    {:ok, table: table}
  end

  defp future_partition(table, months_out) do
    month = Date.utc_today() |> Date.beginning_of_month() |> Date.shift(month: months_out)
    "#{table}_" <> Calendar.strftime(month, "%Y%m")
  end

  describe "missing_partitions/2" do
    test "returns [] when the full horizon is materialised", %{table: t} do
      assert [] = PartitionBootCheck.missing_partitions(t, 2)
    end

    test "detects a dropped future partition", %{table: t} do
      partition = future_partition(t, 2)
      {:ok, _} = Repo.query("DROP TABLE #{partition}")

      assert [month] = PartitionBootCheck.missing_partitions(t, 2)
      assert String.ends_with?(partition, month)
    end
  end

  describe "ensure_partitions!/2" do
    test "recreates dropped partitions", %{table: t} do
      {:ok, _} = Repo.query("DROP TABLE #{future_partition(t, 1)}")
      {:ok, _} = Repo.query("DROP TABLE #{future_partition(t, 2)}")

      assert length(PartitionBootCheck.missing_partitions(t, 2)) == 2
      assert :ok = PartitionBootCheck.ensure_partitions!(t, 2)
      assert [] = PartitionBootCheck.missing_partitions(t, 2)
    end

    test "is a no-op when nothing is missing", %{table: t} do
      assert :ok = PartitionBootCheck.ensure_partitions!(t, 2)
    end
  end

  describe "boot (GenServer.start_link)" do
    test "boot repairs missing partitions automatically (§4.6 acceptance)", %{table: t} do
      {:ok, _} = Repo.query("DROP TABLE #{future_partition(t, 2)}")

      # Start the boot check under supervision. Its init/1 blocks until
      # remediation is complete, if it returns, the partitions are fixed.
      boot_check =
        start_supervised!({PartitionBootCheck, table: t, enabled: true, name: :"#{t}_boot"})

      assert is_pid(boot_check)
      assert [] = PartitionBootCheck.missing_partitions(t, 2)
    end

    test "is disabled by default via app config" do
      #      config/test.exs sets enabled: false, a plain start_link without an
      # override must not touch the DB.
      assert Application.get_env(:ergon, Ergon.PartitionBootCheck)[:enabled] == false
    end

    test "respects enabled: false (no DB touch on boot)", %{table: t} do
      {:ok, _} = Repo.query("DROP TABLE #{future_partition(t, 2)}")

      # enabled: false, init/1 must skip the check entirely.
      start_supervised!({PartitionBootCheck, table: t, enabled: false, name: :"#{t}_disabled"})

      # Partition is still missing, boot check was correctly inert.
      assert [_] = PartitionBootCheck.missing_partitions(t, 2)
    end
  end

  describe "validate_table_name!" do
    # Although the function is private, its contract surfaces through the
    # public API, invalid names must raise rather than reach SQL
    # interpolation.

    test "rejects names with characters outside [a-z0-9_]" do
      assert_raise ArgumentError, ~r/invalid partitioned table name/, fn ->
        PartitionBootCheck.missing_partitions("Evil; DROP TABLE", 2)
      end
    end

    test "rejects names starting with a digit" do
      assert_raise ArgumentError, ~r/invalid partitioned table name/, fn ->
        PartitionBootCheck.missing_partitions("1ev", 2)
      end
    end
  end
end
