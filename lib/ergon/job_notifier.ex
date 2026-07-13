defmodule Ergon.JobNotifier do
  @moduledoc """
  Turns `ergon.jobs` into a reactive queue: a single `LISTEN` connection that
  wakes the right workers the instant a runnable job lands, instead of waiting
  out their poll interval.

  One instance runs per node. It opens a dedicated `Postgrex.Notifications`
  connection (auto-reconnecting) outside `Ergon.Repo`'s pool and `LISTEN`s on
  the fixed channel `ergon_job_available`. The emitting half is the trigger
  installed by `Ergon.Migration.job_notify_trigger/0`, which fires
  `pg_notify('ergon_job_available', NEW.queue)` whenever a job becomes
  immediately runnable (`available`, due, and live). On each notification the
  payload is the **queue name only** (never job data or tenant), and the
  notifier fans a `:wake` out to every worker registered for that queue via
  `Ergon.WorkerRegistry`.

  ## The poll is still the durable path

  `ergon.jobs` itself is the durable fact and `checkout`'s `FOR UPDATE SKIP
  LOCKED` is the reliable puller, so a `NOTIFY` is a hint, not an event that can
  be lost. Workers keep their periodic fallback poll (`Ergon.Queue`'s
  `:poll_interval`), which alone drains everything correctly. That fallback is
  what covers the boot gap before the listener connects, any reconnect window,
  and future-scheduled retries the trigger deliberately does not wake for. A
  dropped or missed notification therefore costs latency, never a stuck job.

  ## Coalescing and scheduled jobs (for free)

  Postgres collapses duplicate `(channel, payload)` `NOTIFY`s within one
  transaction, so a batch enqueue of 100 jobs onto one queue produces a single
  wake. The trigger's `scheduled_at <= now()` guard means backoff/retry rows
  scheduled in the future do not wake anyone early, they are picked up by the
  fallback poll when they come due.

  ## Configuration

  Optional, mirroring `Ergon.Pgmq.Producer`'s `:notify_channel`. Disable it and
  workers simply poll, still fully correct, only slower:

      config :ergon, Ergon.JobNotifier, enabled: false

  ## A note on connection poolers

  `LISTEN` needs a stable, dedicated backend connection, so this module opens
  its own outside `Ergon.Repo`'s pool. But if the host's database URL routes
  through a transaction-mode pooler (e.g. PgBouncer in `transaction` mode),
  that dedicated connection still goes through the pooler and `LISTEN` will
  silently never receive anything. Point the connection config at a
  session-mode route when a transaction-mode pooler sits in front of Postgres.
  """

  use GenServer

  require Logger

  @channel "ergon_job_available"

  @doc "The single `LISTEN`/`NOTIFY` channel job wake-ups travel on."
  @spec channel() :: String.t()
  def channel, do: @channel

  @doc """
  Start the notifier.

  Options:

    * `:repo`, the Ecto repo whose connection config seeds the dedicated
      `LISTEN` connection (default `Ergon.Repo`)
    * `:registry`, the worker registry to dispatch wakes through
      (default `Ergon.WorkerRegistry`)
    * `:name`, the process name (default `Ergon.JobNotifier`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    repo = Keyword.get(opts, :repo, Ergon.Repo)
    registry = Keyword.get(opts, :registry, Ergon.WorkerRegistry)

    conn_opts =
      repo.config()
      |> Keyword.drop([:pool, :pool_size])
      |> Keyword.put(:auto_reconnect, true)

    {:ok, conn} = Postgrex.Notifications.start_link(conn_opts)

    # :eventually just means the LISTEN lands once the (auto-reconnecting)
    # connection is up, until then the workers' fallback poll covers delivery.
    case Postgrex.Notifications.listen(conn, @channel) do
      {:ok, _ref} -> :ok
      {:eventually, _ref} -> :ok
    end

    {:ok, %{conn: conn, registry: registry}}
  end

  @impl true
  def handle_info({:notification, _pid, _ref, _channel, queue}, %{registry: registry} = state) do
    Registry.dispatch(registry, queue, fn entries ->
      for {pid, _value} <- entries, do: send(pid, :wake)
    end)

    {:noreply, state}
  end
end
