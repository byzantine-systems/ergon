defmodule Ergon.ReconcilerTest do
  # Exercises the DR recovery flow.
  #
  # Not async, release_leases and metrics both hit pgmq from the test
  # process, and the stranded-lease state is the test's whole point.
  # `async: false` keeps the sandbox shared and the queue table stable
  # across the run.
  use Ergon.Case, async: false

  @moduletag :integration

  alias Ergon.{Pgmq, Reconciler, Repo}

  setup do
    queue = "reconcile_test_#{System.unique_integer([:positive])}"
    {:ok, _} = Repo.query("SELECT pgmq.create($1)", [queue])
    {:ok, queue: queue}
  end

  defp send!(queue, payload) do
    {:ok, %Postgrex.Result{rows: [[msg_id]]}} =
      Repo.query("SELECT pgmq.send($1, $2)", [queue, payload])

    msg_id
  end

  describe "run/1" do
    test "releases stranded leases and returns a per-queue summary", %{queue: q} do
      msg_id = send!(q, %{"x" => 1})

      # A consumer takes a long lease and dies without acking, the message
      # is now invisible for an hour.
      {:ok, [%{id: ^msg_id, read_ct: 1}]} = Pgmq.read(q, 3600, 10)
      {:ok, []} = Pgmq.read(q, 30, 10)

      summary = Reconciler.run(queues: [q])

      # The reconciler freed exactly one lease and snapshot the depth.
      assert summary.queues[q].released_leases == 1
      assert summary.queues[q].queue_length == 1

      # The stranded message is deliverable again, with read_ct bumped.
      {:ok, [%{id: ^msg_id, read_ct: 2}]} = Pgmq.read(q, 30, 10)
    end

    test "queue stats include the standard pgmq metrics shape", %{queue: q} do
      summary = Reconciler.run(queues: [q])

      assert %{
               released_leases: 0,
               queue_length: 0,
               queue_visible_length: 0,
               oldest_msg_age_sec: nil
             } =
               summary.queues[q]
    end

    test "passes through multiple queues", %{queue: q1} do
      q2 = "reconcile_test_#{System.unique_integer([:positive])}"
      {:ok, _} = Repo.query("SELECT pgmq.create($1)", [q2])

      summary = Reconciler.run(queues: [q1, q2])
      assert Map.keys(summary.queues) |> Enum.sort() == Enum.sort([q1, q2])
    end
  end

  describe "run/1 hydrate callback" do
    test "is invoked and its return value is captured in the summary", %{queue: q} do
      {:ok, agent} = Agent.start_link(fn -> 0 end)

      summary =
        Reconciler.run(
          queues: [q],
          hydrate: fn ->
            Agent.update(agent, &(&1 + 1))
            {:stopped, 3}
          end
        )

      assert summary.hydrate == {:stopped, 3}
      assert Agent.get(agent, & &1) == 1
    end

    test "runs before queue remediation", %{queue: q} do
      # Verify ordering: a hydrate callback that itself reads the queue
      # should still see the stranded lease (release_leases hasn't run yet).
      send!(q, %{"x" => 2})
      {:ok, [_]} = Pgmq.read(q, 3600, 10)

      {seen_during_hydrate, seen_after} =
        Reconciler.run(
          queues: [q],
          hydrate: fn ->
            # The lease is still held when hydrate runs.
            {:ok, visible} = Pgmq.read(q, 30, 10)
            length(visible)
          end
        )
        |> then(fn summary ->
          # After release_leases, the message is visible again.
          {:ok, after_release} = Pgmq.read(q, 30, 10)
          {summary.hydrate, length(after_release)}
        end)

      assert seen_during_hydrate == 0
      assert seen_after == 1
    end

    test "defaults to a no-op when omitted", %{queue: q} do
      summary = Reconciler.run(queues: [q])
      assert summary.hydrate == :ok
    end
  end
end
