defmodule Ergon.IntegrationTest do
  @moduledoc """
  End-to-end tests against a live PostgreSQL 18/19, exercising the temporal
  constraints, the FOR UPDATE SKIP LOCKED checkout, and the SQL/PGQ property
  graph that the pure unit tests cannot reach.
  """
  use Ergon.Case, async: false

  @moduletag :integration

  alias Ergon.{DB, FSM, Graph, NewJob}

  test "enqueue inserts an available job" do
    assert {:ok, job} = Ergon.enqueue(NewJob.new("send_email", %{"to" => "a@b.com"}))
    assert job.state == :available
    assert job.attempt == 0
    assert job.queue == "default"
    # jsonb::text normalizes formatting (a space after each key's colon), so
    # this isn't byte-identical to the JSON we sent in.
    assert job.payload == ~s({"to": "a@b.com"})
  end

  test "checkout claims a job exactly once" do
    {:ok, job} = Ergon.enqueue(NewJob.new("resize") |> NewJob.on_queue("q1"))

    assert {:ok, [claimed]} = DB.checkout("q1", 5)
    assert claimed.id == job.id
    assert claimed.state == :executing
    assert claimed.attempt == 1

    # A second checkout finds nothing live to claim.
    assert {:ok, []} = DB.checkout("q1", 5)
  end

  test "apply_outcome records the transition" do
    {:ok, _job} = Ergon.enqueue(NewJob.new("resize") |> NewJob.on_queue("q2"))
    {:ok, [claimed]} = DB.checkout("q2", 1)

    {:ok, outcome} = FSM.transition(claimed, :succeeded)
    assert {:ok, completed} = DB.apply_outcome(claimed.id, outcome)
    assert completed.state == :completed
  end

  test "a unique job is get-or-create within its window" do
    spec = NewJob.new("report", %{"day" => "2026-07-14"}) |> NewJob.unique_for(3600)

    # A duplicate inside the window returns the existing job (same id), not an
    # error, the ergon.enqueue function absorbs the temporal EXCLUDE conflict.
    assert {:ok, first} = Ergon.enqueue(spec)
    assert {:ok, second} = Ergon.enqueue(spec)
    assert first.id == second.id
  end

  test "a unique job is checkoutable (valid_period stays open)" do
    spec = NewJob.new("digest") |> NewJob.on_queue("uniq_q") |> NewJob.unique_for(3600)
    {:ok, job} = Ergon.enqueue(spec)

    assert {:ok, [claimed]} = DB.checkout("uniq_q", 1)
    assert claimed.id == job.id
    assert claimed.state == :executing
  end

  test "ready_children resolves DAG dependencies through the property graph" do
    # Separate queues so the checkout below deterministically claims the parent
    # and leaves the child untouched (both would otherwise be available).
    {:ok, parent} = Ergon.enqueue(NewJob.new("build") |> NewJob.on_queue("dag_parent"))
    {:ok, child} = Ergon.enqueue(NewJob.new("deploy") |> NewJob.on_queue("dag_child"))
    :ok = Ergon.depends_on(parent.id, child.id)

    # The child is not ready while the parent is still available.
    assert {:ok, ready} = Graph.ready_children()
    refute child.id in ready

    # Complete the parent, now the child is unblocked.
    {:ok, [claimed]} = DB.checkout("dag_parent", 1)
    {:ok, outcome} = FSM.transition(claimed, :succeeded)
    {:ok, _} = DB.apply_outcome(claimed.id, outcome)

    assert {:ok, ready} = Graph.ready_children()
    assert child.id in ready
    assert {:ok, [unblocked]} = Graph.direct_children(parent.id)
    assert unblocked == child.id
  end
end
