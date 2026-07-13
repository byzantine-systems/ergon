defmodule Ergon.HealthTest do
  # Exercises the liveness + diagnostics probe.
  use Ergon.Case, async: true

  @moduletag :integration

  alias Ergon.{Health, Repo}

  describe "check/1" do
    test "returns a map with :db, :extensions, :queues keys" do
      result = Health.check()

      assert Map.has_key?(result, :db)
      assert Map.has_key?(result, :extensions)
      assert Map.has_key?(result, :queues)
    end

    test ":db is {:ok, _} when the pool is reachable" do
      assert {:ok, %Postgrex.Result{rows: [[1]]}} = Health.check().db
    end

    test ":extensions is a %{name => version} map" do
      extensions = Health.check().extensions

      assert is_map(extensions)

      # Phase 1's init migration installs these unconditionally.
      assert Map.has_key?(extensions, "btree_gist")
      assert Map.has_key?(extensions, "pgmq")

      #      pg_cron is installed in dev only, its absence in test verifies the
      # guard. Don't assert on its presence either way, just that if it IS
      # present, the version string is non-empty.
      if version = Map.get(extensions, "pg_cron") do
        assert byte_size(version) > 0
      end
    end

    test ":queues defaults to empty when no queues configured" do
      result = Health.check()
      assert result.queues == %{}
    end

    test ":queues opt returns per-queue metrics" do
      queue = "health_test_#{System.unique_integer([:positive])}"
      {:ok, _} = Repo.query("SELECT pgmq.create($1)", [queue])

      try do
        result = Health.check(queues: [queue])

        assert Map.has_key?(result.queues, queue)
        assert %{queue_length: 0} = result.queues[queue]
      after
        {:ok, _} = Repo.query("SELECT pgmq.drop_queue($1)", [queue])
      end
    end

    test ":queues opt accepts atoms (coerced to string for the metrics key)" do
      queue = "health_test_atom_#{System.unique_integer([:positive])}"
      {:ok, _} = Repo.query("SELECT pgmq.create($1)", [queue])

      try do
        # Pass as atom, Ergon.Health should coerce to the queue's actual name.
        result = Health.check(queues: [String.to_atom(queue)])
        assert Map.has_key?(result.queues, queue)
      after
        {:ok, _} = Repo.query("SELECT pgmq.drop_queue($1)", [queue])
      end
    end
  end
end
