defmodule Ergon.SQLTest do
  # Exercises the filesystem SQL loader, including the
  # acceptance criterion that every statement under priv/queries is accounted
  # for and that the tstzrange decoder wires into Repo.
  use Ergon.Case, async: true

  @moduletag :integration

  alias Ergon.SQL

  describe "loading" do
    test "walks priv/queries and keys files as {:domain, :operation}" do
      keys = SQL.keys()
      assert {:jobs, :insert} in keys
      assert {:jobs, :checkout} in keys
      assert {:jobs, :apply_outcome} in keys
      assert {:jobs, :link} in keys
      assert {:graph, :ready_children} in keys
      assert {:graph, :direct_children} in keys
      assert {:pgmq, :read} in keys
      assert {:pgmq, :archive} in keys
      assert {:pgmq, :metrics} in keys
      assert {:pgmq, :release_leases} in keys
      assert {:partitions, :missing} in keys
      assert {:system, :healthcheck} in keys
      assert {:system, :installed_extensions} in keys
    end

    test "fetch/1 returns the raw statement" do
      assert {:ok, sql} = SQL.fetch({:jobs, :insert})
      assert sql =~ "ergon.enqueue"
    end

    test "fetch!/1 raises a helpful error for an unknown key" do
      assert_raise KeyError, ~r/no SQL registered for \{:nope, :missing\}/, fn ->
        SQL.fetch!({:nope, :missing})
      end
    end

    test "reload/0 re-reads priv/queries and returns the count" do
      assert is_integer(SQL.reload())
      assert length(SQL.keys()) > 0
    end
  end

  describe "query/3" do
    test "runs the named statement on the default repo" do
      # `ready_children` is parameterless and references the property graph
      # installed by the migrations, so it executes cleanly against the schema.
      assert {:ok, %Postgrex.Result{}} = SQL.query({:graph, :ready_children})
    end

    test "query!/3 returns the result directly" do
      assert %Postgrex.Result{} = SQL.query!({:graph, :ready_children})
    end

    test "accepts a repo override opt" do
      assert {:ok, %Postgrex.Result{}} =
               SQL.query({:graph, :ready_children}, [], repo: Ergon.Repo)
    end
  end

  describe "tstzrange decoding" do
    # The custom Postgrex extension must wire `valid_period` (and any other
    # tstzrange) through to `%Ergon.TemporalPeriod{}` rather than the raw
    # `%Postgrex.Range{}` the generic handler would produce.
    test "decodes a tstzrange column into Ergon.TemporalPeriod" do
      assert {:ok, %Postgrex.Result{rows: [[period]]}} =
               Ergon.Repo.query("SELECT tstzrange('2020-01-01+00', '2030-01-01+00', '[)') AS r")

      assert %Ergon.TemporalPeriod{} = period
      assert period.lower == ~U[2020-01-01 00:00:00.000000Z]
      assert period.upper == ~U[2030-01-01 00:00:00.000000Z]
      assert period.lower_inclusive
      refute period.upper_inclusive
      refute period.empty
    end

    test "decodes an unbounded-upper tstzrange (the ergon.jobs shape)" do
      assert {:ok, %Postgrex.Result{rows: [[period]]}} =
               Ergon.Repo.query("SELECT tstzrange(now(), 'infinity', '[)') AS r")

      assert %Ergon.TemporalPeriod{lower: %DateTime{}, upper: :unbounded} = period
    end
  end

  describe "coverage (§4.8 acceptance)" do
    # Every parameterless statement under priv/queries must execute cleanly.
    # Parameterised statements are exercised by their own suites and listed in
    # @parameterized, this guards against orphaned/broken SQL files.
    # {:jobs, :insert} / {:jobs, :checkout} / {:jobs, :apply_outcome} /
    #   {:jobs, :link}, Ergon.IntegrationTest
    # {:graph, :direct_children}, Ergon.IntegrationTest
    # {:pgmq, :read} / {:pgmq, :archive} / {:pgmq, :metrics} /
    #   {:pgmq, :release_leases}, Ergon.PgmqTest
    # {:partitions, :missing}, Ergon.PartitionBootCheckTest
    # {:system, :healthcheck} / {:system, :installed_extensions}, Ergon.HealthTest
    @parameterless [
      {:graph, :ready_children},
      {:system, :healthcheck},
      {:system, :installed_extensions}
    ]

    @parameterized [
      {:jobs, :apply_outcome},
      {:jobs, :asof},
      {:jobs, :asof_system},
      {:jobs, :checkout},
      {:jobs, :discard_descendants},
      {:jobs, :insert},
      {:jobs, :link},
      {:graph, :descendants},
      {:graph, :direct_children},
      {:graph, :would_create_cycle},
      {:pgmq, :read},
      {:pgmq, :archive},
      {:pgmq, :metrics},
      {:pgmq, :release_leases},
      {:partitions, :missing}
    ]

    test "priv/queries has no unaccounted-for statements" do
      assert Enum.sort(SQL.keys()) == Enum.sort(@parameterless ++ @parameterized)
    end

    for key <- @parameterless do
      test "#{inspect(key)} executes against the sandbox" do
        assert {:ok, %Postgrex.Result{}} = SQL.query(unquote(Macro.escape(key)))
      end
    end
  end
end
