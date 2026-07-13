defmodule Ergon.Pgmq.ProducerTest do
  # Exercises the GenStage Broadway.Producer and
  # Broadway.Acknowledger behaviours on Ergon.Pgmq.Producer, message
  # production from pgmq, batch acknowledgement (archive), and the LISTEN
  # fast-path wake-up.
  use Ergon.Case, async: false

  @moduletag :integration

  alias Broadway.Message
  alias Ergon.{Pgmq, Repo}

  setup do
    queue = "pgmq_prod_#{System.unique_integer([:positive])}"
    {:ok, _} = Repo.query("SELECT pgmq.create($1)", [queue])
    {:ok, queue: queue}
  end

  defp send!(queue, payload) do
    {:ok, %Postgrex.Result{rows: [[msg_id]]}} =
      Repo.query("SELECT pgmq.send($1, $2)", [queue, payload])

    msg_id
  end

  defp start_producer!(queue, opts \\ []) do
    {:ok, pid} =
      GenStage.start_link(
        Ergon.Pgmq.Producer,
        [queue: queue, poll_interval: 5] ++ opts
      )

    # The producer runs in a separate process that needs DB access.
    # Grant it sandbox access so pgmq.read/archive go through the test's
    # transactional sandbox instead of hitting a real pool.
    Ecto.Adapters.SQL.Sandbox.allow(Repo, self(), pid)
    pid
  end

  describe "message production" do
    test "produces Broadway.Message structs from pgmq on demand", %{queue: q} do
      msg1 = send!(q, %{"x" => 1})
      msg2 = send!(q, %{"x" => 2})

      producer = start_producer!(q)

      {:ok, consumer} =
        GenStage.start_link(__MODULE__.Consumer, %{parent: self(), ack: false})

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 5)

      assert_receive {:consumer_events, events}, 1000
      ids = for %Message{metadata: %{msg_id: id}} <- events, do: id
      assert Enum.sort(ids) == Enum.sort([msg1, msg2])
    end

    test "produces nothing when the queue is empty", %{queue: q} do
      # Use a very short poll interval and a separate demand window so we have
      # time to assert emptiness between the init poll and any retry.
      producer = start_producer!(q, poll_interval: 50)

      {:ok, consumer} =
        GenStage.start_link(__MODULE__.Consumer, %{parent: self(), ack: false})

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 5)

      refute_receive {:consumer_events, _}, 200
    end
  end

  describe "acknowledgement (Broadway.Acknowledger)" do
    test "ack archives successfully processed messages", %{queue: q} do
      msg_id = send!(q, %{"y" => 1})
      {:ok, _} = Pgmq.read(q, 30, 10)

      message = %Message{
        data: %{"y" => 1},
        acknowledger: {Ergon.Pgmq.Producer, {:pgmq, q, Repo}, msg_id}
      }

      assert :ok = Ergon.Pgmq.Producer.ack({:pgmq, q, Repo}, [message], [])
      assert {:ok, []} = Pgmq.read(q, 30, 10)
    end

    test "failed messages are NOT archived, left for redelivery", %{queue: q} do
      msg_id = send!(q, %{"z" => 1})
      {:ok, _} = Pgmq.read(q, 30, 10)

      message = %Message{
        data: %{"z" => 1},
        acknowledger: {Ergon.Pgmq.Producer, {:pgmq, q, Repo}, msg_id}
      }

      # Only pass the message as failed, it should stay in the queue.
      assert :ok = Ergon.Pgmq.Producer.ack({:pgmq, q, Repo}, [], [message])

      # Still hidden behind its visibility timeout, but not archived.
      assert Pgmq.release_leases(q) == 1
      assert {:ok, [%{id: ^msg_id, read_ct: 2}]} = Pgmq.read(q, 30, 10)
    end

    test "ack with empty lists is a no-op", %{queue: q} do
      assert :ok = Ergon.Pgmq.Producer.ack({:pgmq, q, Repo}, [], [])
    end
  end

  describe "NOTIFY fast-path" do
    test "a notification on the channel triggers an immediate poll", %{queue: q} do
      _ = send!(q, %{"fast" => true})

      # Start the producer, it polls once at init (finding nothing, since
      # no demand has been sent).  Then we inject demand, send a notification,
      # and verify the message arrives without waiting for the poll interval.
      producer = start_producer!(q, poll_interval: 60_000)

      {:ok, consumer} =
        GenStage.start_link(__MODULE__.Consumer, %{parent: self(), ack: false})

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 5)

      # Inject a notification as though `pg_notify` fired.
      # The sandbox rolls back NOTIFY, so we simulate the message that
      # Postgrex.Notifications would forward.
      send(producer, {:notification, :pid, :ref, "pgmq_#{q}", ""})

      assert_receive {:consumer_events, [_]}, 500
    end
  end

  # Minimal GenStage consumer that forwards events to the test process.
  # Passing `ack: true` in the initial state makes it acknowledge messages
  #      (exercising the producer's Acknowledger callback), `ack: false` leaves
  # them in the queue.
  defmodule Consumer do
    use GenStage

    def start_link(state) do
      GenStage.start_link(__MODULE__, state)
    end

    def init(state) do
      {:consumer, state}
    end

    def handle_events(events, _from, state) do
      events = Enum.map(events, &%{&1 | status: {:ok, &1.data}})
      send(state.parent, {:consumer_events, events})

      case state do
        %{ack: true} -> {:noreply, events, state}
        _ -> {:noreply, [], state}
      end
    end
  end
end
