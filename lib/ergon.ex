defmodule Ergon do
  @moduledoc """
  Ergon: PostgreSQL-native background job and workflow processing for Elixir.

  This is the module a host application uses. It exposes the whole lifecycle:
  enqueue jobs, wire up workflow dependencies, ask which jobs are ready, and
  start workers to execute them. Everything runs against the `Ergon.Repo`
  started in the host's supervision tree, schema is installed with the bundled
  migrations (`mix ecto.migrate`).

      {:ok, job} =
        Ergon.NewJob.new("send_email", %{to: "a@b.com"})
        |> Ergon.NewJob.on_queue("mailers")
        |> Ergon.enqueue()

      {:ok, _worker} =
        Ergon.Queue.new("mailers")
        |> Ergon.start_worker(&handle_email/1)

  The design leans entirely on modern PostgreSQL: temporal tables (18/19) for
  unique jobs and auditable history, and SQL/PGQ property graphs (19) for DAG
  dependency resolution. See `Ergon.DB` and `Ergon.Graph`.
  """
  alias Ergon.{DB, Graph, Job, NewJob, Queue, Worker, WorkerSupervisor}

  @doc "Enqueue a job and return the inserted row."
  @spec enqueue(NewJob.t()) :: {:ok, Job.t()} | {:error, DB.error()}
  def enqueue(%NewJob{} = job), do: DB.insert(job)

  @doc """
  Declare that `parent` completing should trigger `child`, adding a `triggers`
  edge to the workflow graph.
  """
  @spec depends_on(integer(), integer()) :: :ok | {:error, DB.error()}
  def depends_on(parent, child), do: DB.link(parent, child, "triggers")

  @doc "Add a labelled dependency edge to the workflow graph."
  @spec link(integer(), integer(), String.t()) :: :ok | {:error, DB.error()}
  def link(parent, child, edge_type), do: DB.link(parent, child, edge_type)

  @doc """
  The ids of every job whose workflow parents have all completed and which is
  therefore ready to be worked.
  """
  @spec ready_children() :: {:ok, [integer()]} | {:error, Exception.t()}
  def ready_children, do: Graph.ready_children()

  @doc "The ids of the available jobs a completed `parent` directly unblocks."
  @spec unblocked_by(integer()) :: {:ok, [integer()]} | {:error, Exception.t()}
  def unblocked_by(parent), do: Graph.direct_children(parent)

  @doc """
  Cancel `job` and cascade the cancellation to every descendant still running
  or waiting, returning the jobs actually discarded.
  """
  @spec cancel(integer()) :: {:ok, [Job.t()]} | {:error, DB.error()}
  def cancel(job), do: DB.cancel_cascade(job)

  @doc "Start a supervised worker that drains `queue`, running `handler` on each job."
  @spec start_worker(Queue.t(), Worker.handler()) :: DynamicSupervisor.on_start_child()
  def start_worker(%Queue{} = queue, handler), do: WorkerSupervisor.start_worker(queue, handler)
end
