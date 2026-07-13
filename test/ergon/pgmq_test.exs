defmodule Ergon.PgmqTest do
  # Exercises the four pgmq wrappers in Ergon.Pgmq
  # against a real pgmq installation. Each test owns its own queue, created
  # inside the sandbox transaction so rollback tears it down, ergon ships no
  #      queues of its own (queues are a host concern, this is exactly the pattern
  # `Ergon.Migration.pgmq_queue/1` exists to support).
  use Ergon.Case, async: true

  @moduletag :integration

  alias Ergon.{Pgmq, Repo}

  # Unique per test so async tests don't contend on the same pgmq metadata /
  # queue table even though their data is sandbox-isolated.
  setup do
    queue = "pgmq_test_#{System.unique_integer([:positive])}"
    {:ok, _} = Repo.query("SELECT pgmq.create($1)", [queue])
    {:ok, queue: queue}
  end

  defp send!(queue, payload) do
    {:ok, %Postgrex.Result{rows: [[msg_id]]}} =
      Repo.query("SELECT pgmq.send($1, $2)", [queue, payload])

    msg_id
  end

  describe "read/3" do
    test "returns the message and hides it behind the visibility timeout", %{queue: queue} do
      payload = %{"ping_id" => 1}
      msg_id = send!(queue, payload)

      assert {:ok, [%{id: ^msg_id, read_ct: 1, message: ^payload}]} = Pgmq.read(queue, 30, 10)

      # Hidden until the timeout expires: a second consumer sees nothing.
      assert {:ok, []} = Pgmq.read(queue, 30, 10)
    end

    test "an expired visibility timeout redelivers with an incremented read_ct", %{queue: queue} do
      msg_id = send!(queue, %{"ping_id" => 2})

      # vt 0 = the lease expires immediately, simulating a consumer that died
      # mid-processing (acceptance: crash → redelivery).
      assert {:ok, [%{id: ^msg_id, read_ct: 1}]} = Pgmq.read(queue, 0, 10)
      assert {:ok, [%{id: ^msg_id, read_ct: 2}]} = Pgmq.read(queue, 30, 10)
    end
  end

  describe "archive/2" do
    test "acks a batch: queue_length drops to zero afterwards", %{queue: queue} do
      ids = for n <- 1..3, do: send!(queue, %{"ping_id" => n})
      assert {:ok, _} = Pgmq.read(queue, 30, 10)

      assert {:ok, archived} = Pgmq.archive(queue, ids)
      assert Enum.sort(archived) == Enum.sort(ids)

      assert %{queue_length: 0} = Pgmq.metrics(queue)
    end

    test "archiving an unknown id returns an empty list rather than raising", %{queue: queue} do
      assert {:ok, []} = Pgmq.archive(queue, [999_999])
    end
  end

  describe "metrics/1" do
    test "reports total queue_length (gotcha: visible is transaction-frozen)", %{queue: queue} do
      send!(queue, %{"ping_id" => 10})
      send!(queue, %{"ping_id" => 11})
      {:ok, [_]} = Pgmq.read(queue, 30, 1)

      # queue_visible_length is computed against transaction-frozen now(), so
      # messages sent inside this sandbox transaction never count as visible
      # here, assert on queue_length only.
      assert %{queue_length: 2, queue_visible_length: _, oldest_msg_age_sec: _} =
               Pgmq.metrics(queue)
    end
  end

  describe "release_leases/1" do
    test "frees stranded messages for immediate redelivery", %{queue: queue} do
      msg_id = send!(queue, %{"ping_id" => 12})

      # A consumer takes a long lease and dies without acking...
      assert {:ok, [%{id: ^msg_id, read_ct: 1}]} = Pgmq.read(queue, 3600, 10)
      assert {:ok, []} = Pgmq.read(queue, 30, 10)

      # ...the reconciler frees it instead of waiting out the hour.
      assert Pgmq.release_leases(queue) == 1
      assert {:ok, [%{id: ^msg_id, read_ct: 2}]} = Pgmq.read(queue, 30, 10)
    end

    test "returns zero when there are no leases to release", %{queue: queue} do
      assert Pgmq.release_leases(queue) == 0
    end
  end
end
