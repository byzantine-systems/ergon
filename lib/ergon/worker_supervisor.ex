defmodule Ergon.WorkerSupervisor do
  @moduledoc """
  The `DynamicSupervisor` under which queue workers run.

  Workers are added at runtime with `start_worker/2` (exposed through
  `Ergon.start_worker/2`) rather than being listed statically, because which
  queues to drain, and with what concurrency, is the host application's call.
  Each worker is supervised independently, so one crashing does not disturb the
  others.
  """
  use DynamicSupervisor

  alias Ergon.{Queue, Worker}

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc "Start a supervised worker draining `queue` with `handler`."
  @spec start_worker(Queue.t(), Worker.handler()) :: DynamicSupervisor.on_start_child()
  def start_worker(%Queue{} = queue, handler) when is_function(handler, 1) do
    spec = %{
      id: {Worker, queue.name},
      start: {Worker, :start_link, [[queue: queue, handler: handler]]},
      restart: :transient
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end
end
