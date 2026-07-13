defmodule Ergon.AdvancedPostgresTest do
  @moduledoc """
  Exercises the PostgreSQL-native features that own invariants and behaviour in
  the database rather than in Elixir: the job_state DOMAIN, the generated
  fingerprint, the transition-guard trigger, retry backoff, recursive-CTE graph
  reachability (descendants / cycle rejection / cascade cancel), and the
  row-level-security tenant isolation.
  """
  use Ergon.Case, async: false

  @moduletag :integration

  alias Ergon.{DB, FSM, Graph, NewJob}

  describe "generated fingerprint (#9)" do
    test "is deterministic and equal for identical (queue, worker, payload)" do
      {:ok, a} = Ergon.enqueue(NewJob.new("w", %{"k" => 1}) |> NewJob.on_queue("fp"))
      {:ok, b} = Ergon.enqueue(NewJob.new("w", %{"k" => 1}) |> NewJob.on_queue("fp"))
      {:ok, c} = Ergon.enqueue(NewJob.new("w", %{"k" => 2}) |> NewJob.on_queue("fp"))

      assert a.fingerprint == b.fingerprint
      refute a.fingerprint == c.fingerprint
      # sha256 hex.
      assert String.match?(a.fingerprint, ~r/\A[0-9a-f]{64}\z/)
    end
  end

  describe "job_state DOMAIN (#1)" do
    test "rejects a state outside the supported set" do
      assert {:error, %Postgrex.Error{postgres: %{code: code}}} =
               Repo.query(
                 "INSERT INTO ergon.jobs (worker, state) VALUES ($1, $2)",
                 ["w", "banana"]
               )

      assert code in [:check_violation, :domain_violation]
    end
  end

  describe "transition guard trigger (#2)" do
    test "rejects an illegal state transition regardless of caller" do
      {:ok, job} = Ergon.enqueue(NewJob.new("w") |> NewJob.on_queue("guard"))
      {:ok, [claimed]} = DB.checkout("guard", 1)
      {:ok, outcome} = FSM.transition(claimed, :succeeded)
      {:ok, completed} = DB.apply_outcome(claimed.id, outcome)
      assert completed.state == :completed

      # completed -> executing is not a legal edge, the DB rejects it even though
      # we bypass Ergon.FSM by hand-crafting the outcome.
      illegal = %FSM.Outcome{state: :executing, attempt: completed.attempt, last_error: nil}

      assert {:error, %Postgrex.Error{postgres: %{message: msg}}} =
               DB.apply_outcome(job.id, illegal)

      assert msg =~ "illegal job transition"
    end
  end

  describe "retry backoff (#6)" do
    test "a retry pushes scheduled_at into the future" do
      {:ok, _} = Ergon.enqueue(NewJob.new("w") |> NewJob.on_queue("backoff"))
      {:ok, [claimed]} = DB.checkout("backoff", 1)

      {:ok, outcome} = FSM.transition(claimed, {:errored, "boom"})
      assert outcome.state == :available
      {:ok, retried} = DB.apply_outcome(claimed.id, outcome)

      assert retried.state == :available
      # scheduled_at moved forward from the original checkout-time schedule.
      assert DateTime.compare(retried.scheduled_at, claimed.scheduled_at) == :gt
    end
  end

  describe "graph reachability (#7)" do
    setup do
      {:ok, a} = Ergon.enqueue(NewJob.new("a") |> NewJob.on_queue("g"))
      {:ok, b} = Ergon.enqueue(NewJob.new("b") |> NewJob.on_queue("g"))
      {:ok, c} = Ergon.enqueue(NewJob.new("c") |> NewJob.on_queue("g"))
      :ok = Ergon.depends_on(a.id, b.id)
      :ok = Ergon.depends_on(b.id, c.id)
      %{a: a, b: b, c: c}
    end

    test "descendants returns the transitive closure", %{a: a, b: b, c: c} do
      assert {:ok, ids} = Graph.descendants(a.id)
      assert Enum.sort(ids) == Enum.sort([b.id, c.id])
    end

    test "link rejects an edge that would create a cycle", %{a: a, c: c} do
      # c -> a would close the a -> b -> c -> a loop.
      assert {:error, :would_create_cycle} = DB.link(c.id, a.id)
      # a self-loop is also rejected.
      assert {:error, :would_create_cycle} = DB.link(a.id, a.id)
    end

    test "cancel_cascade discards the root and its live descendants", %{a: a, b: b, c: c} do
      assert {:ok, discarded} = DB.cancel_cascade(a.id)
      assert Enum.sort(Enum.map(discarded, & &1.id)) == Enum.sort([a.id, b.id, c.id])
      assert Enum.all?(discarded, &(&1.state == :discarded))
    end

    test "cancel_cascade leaves terminal descendants untouched", %{a: a, b: b, c: c} do
      # Take all three into execution, then complete c so the cascade must skip
      # it (completed is terminal) while still discarding a and b.
      {:ok, claimed} = DB.checkout("g", 3)
      cjob = Enum.find(claimed, &(&1.id == c.id))
      {:ok, outcome} = FSM.transition(cjob, :succeeded)
      {:ok, _} = DB.apply_outcome(cjob.id, outcome)

      {:ok, discarded} = DB.cancel_cascade(a.id)
      ids = Enum.map(discarded, & &1.id)
      refute c.id in ids
      assert a.id in ids
      assert b.id in ids
    end
  end

  describe "row-level security tenant isolation (#10)" do
    test "a non-superuser role only sees its own tenant's rows" do
      # ergon (the test role) is superuser and bypasses RLS, so isolation is
      # exercised under a restricted role. Everything runs inside the sandbox
      # transaction, the role is rolled back with it.
      Repo.query!("CREATE ROLE ergon_rls_test NOLOGIN NOBYPASSRLS")
      Repo.query!("GRANT USAGE ON SCHEMA ergon TO ergon_rls_test")
      Repo.query!("GRANT SELECT, INSERT ON ergon.jobs, ergon.jobs_history TO ergon_rls_test")

      Repo.query!("SET LOCAL ROLE ergon_rls_test")
      Repo.query!("SET LOCAL ergon.tenant = 'acme'")
      Repo.query!("INSERT INTO ergon.jobs (worker) VALUES ('acme_job')")

      assert {:ok, %{rows: [[1]]}} = Repo.query("SELECT count(*) FROM ergon.jobs")

      Repo.query!("SET LOCAL ergon.tenant = 'globex'")
      assert {:ok, %{rows: [[0]]}} = Repo.query("SELECT count(*) FROM ergon.jobs")

      Repo.query!("RESET ROLE")
    end
  end
end
