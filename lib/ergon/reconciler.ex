defmodule Ergon.Reconciler do
  @moduledoc """
  Disaster-recovery entry point.

  Reconciles the runtime state layer with what the durable store says, in
  two moves:

    1. Invoke the host-supplied `:hydrate` callback. Ergon doesn't know what
       in-memory state a host keeps (asset trackers, cache projections,
       Broadway pipelines, etc.), the callback is where the host stops any
       suspect processes and rebuilds them from the DB. A no-op default is
       fine for hosts with no in-memory state.

    2. For each pgmq queue, force-release every in-flight visibility lease
       (`Ergon.Pgmq.release_leases/1`) so messages stranded by consumers
       that died mid-processing become immediately deliverable, then
       snapshot the queue metrics.

  Returns a summary map with the `:hydrate` callback's result and a
  per-queue breakdown. Invoked by `mix ergon.reconcile` (when shipped) or
  directly from a remote console on a running node.

  ## Examples

      # Host with an in-memory state holder:
      Ergon.Reconciler.run(
        queues: ~w(telemetry_processing geofence_alerts),
        hydrate: &MyApp.StateTracker.stop_all_and_rebuild/0
      )

      # Host with no in-memory state, pgmq-only recovery:
      Ergon.Reconciler.run(queues: ~w(jobs))
  """

  require Logger

  alias Ergon.Pgmq

  @type queue_stats :: %{
          released_leases: non_neg_integer(),
          queue_length: non_neg_integer(),
          queue_visible_length: non_neg_integer(),
          oldest_msg_age_sec: number() | nil
        }

  @type summary :: %{
          hydrate: term(),
          queues: %{String.t() => queue_stats()}
        }

  @doc """
  Run the reconciliation flow. See the module docs for the contract.

  ## Options

    * `:queues`, list of pgmq queue names to release and snapshot (default `[]`)
    * `:hydrate`, zero-arity callback to stop/rebuild host-side state
      (default `fn -> :ok end`)
    * `:repo`, Ecto repo for pgmq operations (defaults to `Ergon.Repo`)
  """
  @spec run(keyword()) :: summary()
  def run(opts \\ []) do
    queues = Keyword.get(opts, :queues, [])
    hydrate = Keyword.get(opts, :hydrate, fn -> :ok end)
    pgmq_opts = Keyword.take(opts, [:repo])

    # Host-side state first: stop suspect processes before releasing
    # messages, otherwise the redelivered messages land on consumers that
    # are about to be killed anyway.
    hydrate_result = hydrate.()
    queue_stats = Map.new(queues, &{&1, inspect_queue(&1, pgmq_opts)})

    summary = %{hydrate: hydrate_result, queues: queue_stats}

    Logger.info(
      "Ergon.Reconciler: hydrate=#{inspect(hydrate_result)}. " <>
        Enum.map_join(queue_stats, ", ", fn {queue, stats} ->
          "#{queue}: depth=#{stats.queue_length} (#{stats.released_leases} lease(s) released)"
        end)
    )

    summary
  end

  defp inspect_queue(queue, opts) do
    # Release stranded leases first, then snapshot, the metrics then
    # reflect the post-recovery state.
    released = Pgmq.release_leases(queue, opts)
    metrics = Pgmq.metrics(queue, opts)

    Map.merge(metrics, %{released_leases: released})
  end
end
