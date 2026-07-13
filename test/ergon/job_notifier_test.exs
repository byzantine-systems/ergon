defmodule Ergon.JobNotifierTest do
  # Exercises the LISTEN/NOTIFY wake-up path end to end: the trigger on
  # ergon.jobs firing pg_notify, and Ergon.JobNotifier routing the payload's
  # queue to the workers registered for it.
  #
  # NOTIFY only delivers once the emitting transaction commits, so unlike the
  # rest of the suite these tests do NOT run inside the SQL sandbox. They use a
  # dedicated committed connection with a per-test unique queue prefix and clean
  # up their own rows on exit. async: false so the committed rows never overlap
  # another test in this file.
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Ergon.{JobNotifier, Repo, WorkerRegistry}

  setup do
    conn_opts = Repo.config() |> Keyword.drop([:pool, :pool_size])
    {:ok, conn} = Postgrex.start_link(conn_opts)
    prefix = "jobnotif_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # `conn` is linked to the (now-exiting) test process, so clean up on a
      # fresh short-lived connection. DELETE fires the versioning trigger, which
      # archives into jobs_history, so drop the history rows afterwards too.
      {:ok, cleanup} = Postgrex.start_link(conn_opts)
      Postgrex.query!(cleanup, "DELETE FROM ergon.jobs WHERE queue LIKE $1", [prefix <> "%"])

      Postgrex.query!(cleanup, "DELETE FROM ergon.jobs_history WHERE queue LIKE $1", [prefix <> "%"])
    end)

    {:ok, conn: conn, conn_opts: conn_opts, prefix: prefix}
  end

  describe "routing" do
    test "dispatches :wake to every worker registered for the payload's queue", %{prefix: prefix} do
      queue = "#{prefix}_route"

      # Two "workers" (this test process, twice, via two registrations) on the
      # same queue both wake, mirroring several workers draining one queue.
      {:ok, _} = WorkerRegistry.register(queue)

      task =
        Task.async(fn ->
          WorkerRegistry.register(queue)
          assert_receive :wake, 1000
        end)

      # Let the task register before the dispatch.
      Process.sleep(50)

      notifier = start_supervised!(JobNotifier)

      # Inject the notification directly (deterministic, no commit race), the
      # same shape Postgrex.Notifications would forward. Payload = queue name.
      send(notifier, {:notification, :pid, :ref, JobNotifier.channel(), queue})

      assert_receive :wake, 1000
      Task.await(task)
    end

    test "a notification for an unregistered queue wakes no one", %{prefix: prefix} do
      {:ok, _} = WorkerRegistry.register("#{prefix}_mine")
      notifier = start_supervised!(JobNotifier)

      send(notifier, {:notification, :pid, :ref, JobNotifier.channel(), "#{prefix}_other"})

      refute_receive :wake, 300
    end
  end

  describe "trigger → NOTIFY (committed, end to end)" do
    test "an enqueued runnable job notifies with the queue name", %{
      conn: conn,
      conn_opts: conn_opts,
      prefix: prefix
    } do
      {:ok, notif} = Postgrex.Notifications.start_link(conn_opts)
      listen!(notif, JobNotifier.channel())

      queue = "#{prefix}_run"

      {:ok, _} =
        Postgrex.query(conn, "INSERT INTO ergon.jobs (queue, worker) VALUES ($1, $2)", [
          queue,
          "w"
        ])

      assert_receive {:notification, _pid, _ref, channel, ^queue}, 2000
      assert channel == JobNotifier.channel()
    end

    test "a future-scheduled job does NOT notify (the guard holds)", %{
      conn: conn,
      conn_opts: conn_opts,
      prefix: prefix
    } do
      {:ok, notif} = Postgrex.Notifications.start_link(conn_opts)
      listen!(notif, JobNotifier.channel())

      future = "#{prefix}_future"

      {:ok, _} =
        Postgrex.query(
          conn,
          "INSERT INTO ergon.jobs (queue, worker, scheduled_at) VALUES ($1, $2, now() + interval '1 hour')",
          [future, "w"]
        )

      # The trigger's `scheduled_at <= now()` guard means a backoff/retry row
      # scheduled in the future wakes no one, the fallback poll picks it up when
      # it comes due.
      refute_receive {:notification, _pid, _ref, _channel, ^future}, 500
    end
  end

  # Block until the LISTEN is actually registered on a live connection, so a
  # committed NOTIFY that follows is guaranteed to be delivered ({:eventually,
  # _} means the connection isn't up yet).
  defp listen!(notif, channel) do
    case Postgrex.Notifications.listen(notif, channel) do
      {:ok, _ref} ->
        :ok

      {:eventually, _ref} ->
        Process.sleep(50)
        listen!(notif, channel)
    end
  end
end
