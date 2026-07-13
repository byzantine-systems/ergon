defmodule Ergon.JobsBitemporalTest do
  # The bi-temporal invariant on ergon.jobs.
  #
  # Every state mutation (UPDATE or DELETE) must archive exactly one row into
  # ergon.jobs_history with a closed, non-empty system_time window. The
  # clock_timestamp() choice in temporal_versioning() is what guarantees the
  # "non-empty" half, now() is frozen for the whole transaction, so an
  # insert+update inside one transaction would close the window at its own
  # lower bound and produce an invisible system_time range.
  use Ergon.Case, async: true

  @moduletag :integration

  alias Ergon.{DB, FSM, NewJob, Repo}

  describe "Phase 2 schema additions" do
    test "ergon.jobs has a system_time column" do
      assert {:ok, %Postgrex.Result{rows: [[1]]}} =
               Repo.query("""
               SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'ergon' AND table_name = 'jobs'
                 AND column_name = 'system_time'
               """)
    end

    test "ergon.jobs_history exists" do
      assert {:ok, %Postgrex.Result{rows: [["ergon.jobs_history"]]}} =
               Repo.query("SELECT to_regclass('ergon.jobs_history')::text")
    end

    test "the versioning trigger is attached" do
      assert {:ok, %Postgrex.Result{rows: [[1]]}} =
               Repo.query("SELECT 1 FROM pg_trigger WHERE tgname = 'jobs_versioning_trigger'")
    end

    test "jobs_history_id_system_time_idx is a GiST index on (id, system_time)" do
      assert {:ok, %Postgrex.Result{rows: [[1]]}} =
               Repo.query("""
               SELECT 1 FROM pg_indexes
               WHERE schemaname = 'ergon'
                 AND indexname = 'jobs_history_id_system_time_idx'
                 AND indexdef ILIKE '%USING gist%id%system_time%'
               """)
    end
  end

  describe "system-time versioning" do
    test "INSERT stamps system_time as [now, unbounded) and archives nothing" do
      {:ok, job} = Ergon.enqueue(NewJob.new("resize"))

      # INSERT is not an UPDATE/DELETE, so the trigger only sets the open
      # system_time on the new row, no history row yet.
      assert {:ok, %Postgrex.Result{rows: [[true]]}} =
               Repo.query(
                 "SELECT upper_inf(system_time) FROM ergon.jobs WHERE id = $1",
                 [job.id]
               )

      assert {:ok, %Postgrex.Result{rows: [[0]]}} =
               Repo.query(
                 "SELECT count(*) FROM ergon.jobs_history WHERE id = $1",
                 [job.id]
               )
    end

    test "a plain UPDATE transition (DB.checkout) archives one closed system_time row" do
      {:ok, job} = Ergon.enqueue(NewJob.new("resize") |> NewJob.on_queue("bt1"))
      {:ok, [claimed]} = DB.checkout("bt1", 1)
      _ = claimed

      # checkout.sql issues UPDATE ergon.jobs SET state='executing', attempt=...
      # (no FOR PORTION OF). The trigger archives the prior (available) row
      #      with closed system_time, the live row keeps upper_inf(system_time).
      assert {:ok, %Postgrex.Result{rows: [[1, false, true]]}} =
               Repo.query(
                 "SELECT count(*), bool_or(upper_inf(system_time)), " <>
                   "bool_and(lower(system_time) < upper(system_time)) " <>
                   "FROM ergon.jobs_history WHERE id = $1",
                 [job.id]
               )
    end

    test "a FOR PORTION OF transition (DB.apply_outcome) archives another closed row" do
      {:ok, job} = Ergon.enqueue(NewJob.new("resize") |> NewJob.on_queue("bt2"))
      {:ok, [claimed]} = DB.checkout("bt2", 1)
      {:ok, outcome} = FSM.transition(claimed, :succeeded)
      {:ok, _} = DB.apply_outcome(claimed.id, outcome)

      # Two transitions total (checkout + apply_outcome) → two archived rows,
      # each with a closed, non-empty system_time window. The non-empty half
      # is the clock_timestamp() contract: now() would have frozen across the
      # sandbox transaction, producing an invisible (empty) system_time for
      # any transition after the first.
      assert {:ok, %Postgrex.Result{rows: [[2, false, true]]}} =
               Repo.query(
                 "SELECT count(*), bool_or(upper_inf(system_time)), " <>
                   "bool_and(lower(system_time) < upper(system_time)) " <>
                   "FROM ergon.jobs_history WHERE id = $1",
                 [job.id]
               )

      # Archived windows are contiguous (each prior belief ends exactly where
      # the next begins), i.e. they tile the timeline without overlaps or
      # gaps. Computed via a subquery because Postgres forbids window-function
      # arguments inside an aggregate.
      assert {:ok, %Postgrex.Result{rows: [[true]]}} =
               Repo.query(
                 "SELECT bool_or(prev_upper = lower(system_time)) FROM (" <>
                   "SELECT system_time, " <>
                   "coalesce(lag(upper(system_time)) OVER (ORDER BY lower(system_time)), " <>
                   "lower(system_time)) AS prev_upper " <>
                   "FROM ergon.jobs_history WHERE id = $1" <>
                   ") s",
                 [job.id]
               )
    end
  end
end
