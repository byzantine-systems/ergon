defmodule Ergon.Worker do
  @moduledoc """
  The polling execution loop for one queue, implemented as a `GenServer`.

  On each tick a worker checks out a batch of jobs, runs the user's handler
  against each, threads the result through the pure `Ergon.FSM` state machine,
  and persists the outcome with `Ergon.DB.apply_outcome/2` before scheduling
  the next tick. One worker drains its queue sequentially. Run several (via
  `Ergon.start_worker/2`) for concurrency.

  The periodic poll is the reliable fallback. On init a worker also registers
  in `Ergon.WorkerRegistry` under its queue name, so `Ergon.JobNotifier` can
  send it a `:wake` the moment a job lands and it drains immediately rather than
  waiting out `poll_interval`. With the notifier disabled or a wake lost, the
  fallback poll still drains everything, only later.
  """
  use GenServer

  require Logger
  alias Ergon.{DB, FSM, Job, Queue, WorkerRegistry}

  @typedoc """
  A user-supplied job handler. Returning `:ok` completes the job, while returning
  `{:error, reason}` records the reason and retries until attempts are spent.
  """
  @type handler :: (Job.t() -> :ok | {:error, String.t()})

  @doc """
  Start a worker draining `queue`, running `handler` against each job.

  Expects a keyword list with `:queue` (an `Ergon.Queue`) and `:handler`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    queue = Keyword.fetch!(opts, :queue)

    # Register for NOTIFY-driven wake-ups. Harmless if the notifier is disabled,
    # the registry is always up (started ahead of workers in Ergon.Application).
    WorkerRegistry.register(queue.name)

    state = %{
      queue: queue,
      handler: Keyword.fetch!(opts, :handler)
    }

    # Kick off the first poll as soon as the worker is running.
    {:ok, state, {:continue, :poll}}
  end

  @impl true
  def handle_continue(:poll, state), do: poll(state)

  @impl true
  def handle_info(:poll, state), do: poll(state)

  # Fast-path wake-up from Ergon.JobNotifier: a job just landed on this queue,
  # so drain now rather than waiting out the interval. No timer is scheduled
  # here, the existing :poll loop keeps running as the fallback untouched.
  def handle_info(:wake, state) do
    drain(state)
    {:noreply, state}
  end

  defp poll(%{queue: %Queue{} = queue} = state) do
    drain(state)
    # Schedule the next tick, so a backlog is worked one batch per interval.
    Process.send_after(self(), :poll, queue.poll_interval)
    {:noreply, state}
  end

  defp drain(%{queue: queue} = state) do
    case DB.checkout(queue.name, queue.batch_size) do
      {:ok, jobs} ->
        Enum.each(jobs, &run(state, &1))

      {:error, reason} ->
        # A transient checkout failure is skipped, and the next tick retries.
        Logger.warning("ergon worker checkout failed on #{queue.name}: #{inspect(reason)}")
    end
  end

  defp run(%{handler: handler}, %Job{} = job) do
    event =
      case safe_run(handler, job) do
        :ok -> :succeeded
        {:error, reason} -> {:errored, reason}
      end

    with {:ok, outcome} <- FSM.transition(job, event),
         {:ok, _persisted} <- DB.apply_outcome(job.id, outcome) do
      :ok
    else
      # Leave reconciliation to the next poll or an operator rather than
      # crashing the worker over a single job.
      other -> Logger.warning("ergon worker could not finalise job #{job.id}: #{inspect(other)}")
    end
  end

  # A crashing handler is treated as an ordinary error so one bad job never
  # takes the whole worker down with it.
  defp safe_run(handler, job) do
    case handler.(job) do
      :ok -> :ok
      {:error, reason} when is_binary(reason) -> {:error, reason}
      other -> {:error, "handler returned #{inspect(other)}"}
    end
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    kind, value -> {:error, "#{kind}: #{inspect(value)}"}
  end
end
