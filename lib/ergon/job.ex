defmodule Ergon.Job do
  @moduledoc """
  A fully materialised job row, as returned by the database, plus the helpers
  that translate between the `state` text column and the atoms the state
  machine speaks.

  This struct is the shared vocabulary of the whole library: `Ergon.DB`,
  `Ergon.FSM`, `Ergon.Worker`, and the top-level `Ergon` module all pass
  `%Ergon.Job{}` values around. Construct one only via `from_row/1`, which
  decodes a Postgrex row projected with `column_list/0`.
  """

  @enforce_keys [
    :id,
    :queue,
    :worker,
    :payload,
    :state,
    :fingerprint,
    :attempt,
    :max_attempts,
    :scheduled_at,
    :inserted_at
  ]
  defstruct [
    :id,
    :queue,
    :worker,
    :payload,
    :state,
    :fingerprint,
    :attempt,
    :max_attempts,
    :last_error,
    :scheduled_at,
    :inserted_at
  ]

  @typedoc "The lifecycle state of a job. Mirrors the `state` text column."
  @type state :: :available | :executing | :completed | :failed | :discarded

  @type t :: %__MODULE__{
          id: integer(),
          queue: String.t(),
          worker: String.t(),
          payload: String.t(),
          state: state(),
          fingerprint: String.t(),
          attempt: non_neg_integer(),
          max_attempts: pos_integer(),
          last_error: String.t() | nil,
          scheduled_at: DateTime.t(),
          inserted_at: DateTime.t()
        }

  @states %{
    "available" => :available,
    "executing" => :executing,
    "completed" => :completed,
    "failed" => :failed,
    "discarded" => :discarded
  }
  @state_strings Map.new(@states, fn {string, atom} -> {atom, string} end)

  @doc """
  The column list, in order, that every job-returning query must project so
  that `from_row/1` lines up with the positional row Postgrex returns.
  """
  @spec column_list() :: String.t()
  def column_list do
    "id, queue, worker, payload::text AS payload, state, fingerprint, " <>
      "attempt, max_attempts, last_error, scheduled_at, inserted_at"
  end

  @doc "Decode a Postgrex row (projected with `column_list/0`) into a `%Ergon.Job{}`."
  @spec from_row([term()]) :: t()
  def from_row([
        id,
        queue,
        worker,
        payload,
        state,
        fingerprint,
        attempt,
        max_attempts,
        last_error,
        scheduled_at,
        inserted_at
      ]) do
    %__MODULE__{
      id: id,
      queue: queue,
      worker: worker,
      payload: payload,
      state: state_from_string!(state),
      fingerprint: fingerprint,
      attempt: attempt,
      max_attempts: max_attempts,
      last_error: last_error,
      scheduled_at: scheduled_at,
      inserted_at: inserted_at
    }
  end

  @doc "The text form of a job state, as stored in the database."
  @spec state_to_string(state()) :: String.t()
  def state_to_string(state) when is_map_key(@state_strings, state), do: @state_strings[state]

  @doc "Parse a state string, returning `:error` for an unknown value."
  @spec state_from_string(String.t()) :: {:ok, state()} | :error
  def state_from_string(raw) do
    case Map.fetch(@states, raw) do
      {:ok, state} -> {:ok, state}
      :error -> :error
    end
  end

  defp state_from_string!(raw) do
    case state_from_string(raw) do
      {:ok, state} -> state
      :error -> raise ArgumentError, "unknown job state: #{inspect(raw)}"
    end
  end
end
