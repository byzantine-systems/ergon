defmodule Ergon.Graph do
  @moduledoc """
  Workflow dependency resolution over PostgreSQL 19's SQL/PGQ property graph.

  The `ergon.workflow` property graph (defined in the migrations) maps the
  relational job and edge tables into a graph of `job` vertices connected by
  `triggers` edges. That lets a single `GRAPH_TABLE ... MATCH` query answer
  "which jobs are now ready to run" instead of hand-rolled recursive CTEs or
  per-job dependency checks in application code.
  """
  alias Ergon.SQL

  @doc """
  The ids of every `available` job whose parents in the workflow graph have
  *all* completed, and which may therefore now be checked out.
  """
  @spec ready_children() :: {:ok, [integer()]} | {:error, Exception.t()}
  def ready_children do
    with {:ok, %{rows: rows}} <- SQL.query({:graph, :ready_children}) do
      {:ok, List.flatten(rows)}
    end
  end

  @doc """
  The ids of the direct children a completed job unblocks: every `available`
  job reachable in one `triggers` hop from `parent_id`.
  """
  @spec direct_children(integer()) :: {:ok, [integer()]} | {:error, Exception.t()}
  def direct_children(parent_id) do
    with {:ok, %{rows: rows}} <- SQL.query({:graph, :direct_children}, [parent_id]) do
      {:ok, List.flatten(rows)}
    end
  end

  @doc """
  Every job id reachable from `ancestor_id` through `triggers` edges (its
  transitive closure). Backs cascade operations like `Ergon.DB.cancel_cascade/1`.

  Uses a recursive CTE rather than a `GRAPH_TABLE` walk, PostgreSQL 19's SQL/PGQ
  does not yet support the path quantifiers a variable-length reachability query
  needs.
  """
  @spec descendants(integer()) :: {:ok, [integer()]} | {:error, Exception.t()}
  def descendants(ancestor_id) do
    with {:ok, %{rows: rows}} <- SQL.query({:graph, :descendants}, [ancestor_id]) do
      {:ok, List.flatten(rows)}
    end
  end

  @doc """
  Whether adding the edge `parent_id -> child_id` would introduce a cycle
  (including a self-loop). `Ergon.DB.link/3` calls this to keep the workflow a
  DAG.
  """
  @spec would_create_cycle?(integer(), integer()) :: {:ok, boolean()} | {:error, Exception.t()}
  def would_create_cycle?(parent_id, child_id) do
    case SQL.query({:graph, :would_create_cycle}, [parent_id, child_id]) do
      {:ok, %{rows: [[cycle?]]}} -> {:ok, cycle?}
      {:error, _} = error -> error
    end
  end
end
