defmodule Ergon.Pgmq.Producer do
  @moduledoc """
  Broadway producer + acknowledger over a pgmq queue.

  While Broadway demand is outstanding, polls `pgmq.read/3` on the configured
  repo every `:poll_interval` ms (default 100), wrapping each queued message in
  a `Broadway.Message`. Successfully processed messages are acked in batch via
  `pgmq.archive`. Failed messages are deliberately left untouched so their
  visibility timeout (`:visibility_timeout` seconds, default 30) expires and
  pgmq redelivers them, at-least-once delivery that survives BEAM crashes.

  The poll is the source-of-truth delivery mechanism. With `:notify_channel`
  set, a `LISTEN` connection (`Postgrex.Notifications`, auto-reconnecting) is
  layered on top purely as a fast-path wake-up: a notification triggers an
  immediate poll instead of waiting out the interval. Notifications carry no
  data and may be lost freely, pgmq is the durable buffer, so a dropped
  `NOTIFY` costs latency, never events.

  Something still has to call `pg_notify` when a message lands, that's
  `Ergon.Migration.pgmq_notify_trigger/2`, which installs a trigger on
  `pgmq.q_<queue>` firing on every insert, regardless of what inserted it.
  Without running that migration helper, `:notify_channel` has nothing to
  listen for and the producer just polls on `:poll_interval` the whole time
  (still correct, just not fast).

  ## Options

    * `:queue`, pgmq queue name (required)
    * `:repo`, Ecto repo for pgmq operations (defaults to `Ergon.Repo`;
      high-throughput host apps should configure a separate pool)
    * `:poll_interval`, ms between polls while demand is unmet (default 100)
    * `:visibility_timeout`, seconds a read message stays hidden (default 30)
    * `:notify_channel`, optional LISTEN channel for the wake-up fast path
      (convention: `pgmq_<queue>`, installed by
      `Ergon.Migration.pgmq_notify_trigger/2`)

  ## A note on connection poolers

  `LISTEN` needs a stable, dedicated backend connection, this module opens
  its own outside `Ergon.Repo`'s pool for exactly that reason. But if the
  host's database URL routes through a transaction-mode pooler (e.g.
  PgBouncer in `transaction` pooling mode), that dedicated connection is
  still going through the pooler and `LISTEN` will silently never receive
  anything. Point `:repo`'s connection config (or a separate one passed to
  this module) at a session-mode route when a transaction-mode pooler sits
  in front of Postgres.
  """

  use GenStage

  @behaviour Broadway.Producer
  @behaviour Broadway.Acknowledger

  alias Broadway.Message
  alias Ergon.SQL

  @impl GenStage
  def init(opts) do
    repo = Keyword.get(opts, :repo, Ergon.Repo)

    state = %{
      repo: repo,
      queue: Keyword.fetch!(opts, :queue),
      poll_interval: Keyword.get(opts, :poll_interval, 100),
      visibility_timeout: Keyword.get(opts, :visibility_timeout, 30),
      demand: 0,
      poll_scheduled?: false
    }

    maybe_listen(Keyword.get(opts, :notify_channel), repo)
    {:producer, state}
  end

  # The listener is linked: if it dies the producer restarts with it. Its own
  # auto_reconnect handles DB blips (including re-issuing the LISTEN), and the
  # polling loop keeps delivering regardless.
  defp maybe_listen(nil, _repo), do: :ok

  defp maybe_listen(channel, repo) do
    opts =
      repo.config()
      |> Keyword.drop([:pool, :pool_size])
      |> Keyword.put(:auto_reconnect, true)

    {:ok, pid} = Postgrex.Notifications.start_link(opts)

    # :eventually, the LISTEN lands once the (auto-reconnecting) connection is
    # up, but until then the polling loop covers delivery anyway.
    case Postgrex.Notifications.listen(pid, channel) do
      {:ok, _ref} -> :ok
      {:eventually, _ref} -> :ok
    end
  end

  @impl GenStage
  def handle_demand(incoming, state) do
    poll(%{state | demand: state.demand + incoming})
  end

  @impl GenStage
  def handle_info(:poll, state) do
    poll(%{state | poll_scheduled?: false})
  end

  # Fast-path wake-up: something was just enqueued, poll now rather than
  # waiting out the interval. Demand-less notifications are dropped, the next
  # handle_demand polls anyway.
  def handle_info({:notification, _pid, _ref, _channel, _payload}, state) do
    poll(state)
  end

  defp poll(%{demand: 0} = state), do: {:noreply, [], state}

  defp poll(state) do
    messages = read(state)
    state = %{state | demand: state.demand - length(messages)}
    {:noreply, messages, schedule_poll(state)}
  end

  defp read(%{queue: queue, visibility_timeout: vt, demand: demand, repo: repo}) do
    %Postgrex.Result{rows: rows} =
      SQL.query!({:pgmq, :read}, [queue, vt, demand], repo: repo)

    for [msg_id, read_ct, payload] <- rows do
      %Message{
        data: payload,
        metadata: %{msg_id: msg_id, read_ct: read_ct},
        acknowledger: {__MODULE__, {:pgmq, queue, repo}, msg_id}
      }
    end
  end

  # One timer at a time, so nothing to poll for once demand is satisfied (fresh
  # demand triggers an immediate poll via handle_demand).
  defp schedule_poll(%{poll_scheduled?: true} = state), do: state
  defp schedule_poll(%{demand: 0} = state), do: state

  defp schedule_poll(state) do
    Process.send_after(self(), :poll, state.poll_interval)
    %{state | poll_scheduled?: true}
  end

  @impl Broadway.Acknowledger
  def ack({:pgmq, queue, repo}, successful, _failed) do
    case for %Message{acknowledger: {_, _, msg_id}} <- successful, do: msg_id do
      [] -> :ok
      msg_ids -> SQL.query!({:pgmq, :archive}, [queue, msg_ids], repo: repo)
    end

    :ok
  end
end
