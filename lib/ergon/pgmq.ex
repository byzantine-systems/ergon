defmodule Ergon.Pgmq do
  @moduledoc """
  Thin wrappers over `pgmq` SQL functions.

  pgmq is the durable-queue transport: every host app that wants Broadway-style
  at-least-once streaming creates its own queues via `Ergon.Migration.pgmq_queue/1`
  and feeds them with raw `pgmq.send/2`. This module is the read/archive side
  that consumers (notably `Ergon.Pgmq.Producer` in Phase 4) and operators
  (`Ergon.Reconciler` / `Ergon.Health` in Phase 7) call.

  Each function delegates to a single statement file under
  `priv/queries/pgmq/`, executed via `Ergon.SQL`. The contracts match the
  underlying pgmq functions exactly, see the SQL files for parameter order
  and rationale.
  """

  alias Ergon.SQL

  @type queue :: String.t() | atom()
  @type msg :: %{id: integer(), read_ct: integer(), message: term()}
  @type metrics :: %{
          queue_length: non_neg_integer(),
          queue_visible_length: non_neg_integer(),
          oldest_msg_age_sec: number() | nil
        }
  @doc """
  Read up to `limit` messages from `queue`, hiding each behind a visibility
  timeout of `vt_seconds`. A message that isn't archived before the timeout
  expires becomes visible again and is redelivered, this is the at-least-once
  guarantee.

  Options: `:repo` (defaults to `Ergon.Repo`).
  """
  @spec read(queue(), pos_integer(), pos_integer(), keyword()) :: {:ok, [msg()]}
  def read(queue, vt_seconds, limit, opts \\ [])
      when is_integer(vt_seconds) and is_integer(limit) do
    %Postgrex.Result{rows: rows} =
      SQL.query!({:pgmq, :read}, [normalize(queue), vt_seconds, limit], opts)

    {:ok, Enum.map(rows, &row_to_msg/1)}
  end

  @doc """
  Archive (ack) `msg_ids` on `queue`. Archived messages move from
  `pgmq.q_<queue>` to the `pgmq.a_<queue>` audit table, leaving a durable
  trail. Returns the ids actually archived (already-archived ids are silently
  absent).

  Options: `:repo` (defaults to `Ergon.Repo`).
  """
  @spec archive(queue(), [integer()], keyword()) :: {:ok, [integer()]}
  def archive(queue, msg_ids, opts \\ []) when is_list(msg_ids) do
    %Postgrex.Result{rows: rows} =
      SQL.query!({:pgmq, :archive}, [normalize(queue), msg_ids], opts)

    {:ok, List.flatten(rows)}
  end

  @doc """
  Health snapshot of one queue: total length, visible length, oldest message
  age. Used by `Ergon.Health` and the reconciler.

  Note: `queue_visible_length` is computed against transaction-frozen `now()`,
  so messages sent inside the same transaction read as invisible. Assert on
  `queue_length` in sandbox tests.

  Options: `:repo` (defaults to `Ergon.Repo`).
  """
  @spec metrics(queue(), keyword()) :: metrics()
  def metrics(queue, opts \\ []) do
    %Postgrex.Result{rows: [[length, visible, oldest] | _]} =
      SQL.query!({:pgmq, :metrics}, [normalize(queue)], opts)

    %{queue_length: length, queue_visible_length: visible, oldest_msg_age_sec: oldest}
  end

  @doc """
  Force-expire every in-flight visibility lease on `queue`, making the held
  messages immediately re-readable. Recovery tool for messages stranded by
  consumers that died mid-processing, instead of waiting out each visibility
  timeout, the reconciler frees them all in one shot. Returns the number of
  leases released.

  Options: `:repo` (defaults to `Ergon.Repo`).
  """
  @spec release_leases(queue(), keyword()) :: non_neg_integer()
  def release_leases(queue, opts \\ []) do
    %Postgrex.Result{rows: [[released]]} =
      SQL.query!({:pgmq, :release_leases}, [normalize(queue)], opts)

    released
  end

  defp normalize(queue) when is_binary(queue), do: queue
  defp normalize(queue) when is_atom(queue), do: Atom.to_string(queue)

  defp row_to_msg([msg_id, read_ct, message]),
    do: %{id: msg_id, read_ct: read_ct, message: message}
end
