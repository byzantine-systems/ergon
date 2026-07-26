defmodule Ergon.Queue do
  @moduledoc """
  Runtime configuration for a single named queue: how often a worker polls it
  and how many jobs it takes per poll. Build with `new/1` plus the setters so
  call sites are unaffected when new tuning knobs are added.

  The default `poll_interval` is 5 s: with `Ergon.JobNotifier` running and
  pg_cron installed, a runnable job wakes its workers over `LISTEN`/`NOTIFY`
  within the ~1 s notifier tick, so the poll is only the fallback that catches
  the boot gap and reconnect windows. Where pg_cron is absent (no NOTIFY ever
  fires) the poll is the *only* wake path, which is why the default stays
  moderate rather than long. Raise it with `with_poll_interval/2` on
  pg_cron-backed deployments, or lower it if the notifier is disabled and you
  need tight latency from polling alone.
  """

  @enforce_keys [:name]
  defstruct name: nil,
            poll_interval: 5_000,
            batch_size: 1

  @type t :: %__MODULE__{
          name: String.t(),
          poll_interval: pos_integer(),
          batch_size: pos_integer()
        }

  @doc "A queue named `name`, polled every 5 s (the fallback), taking one job at a time."
  @spec new(String.t()) :: t()
  def new(name) when is_binary(name), do: %__MODULE__{name: name}

  @doc "Set how long the worker waits between polls, in milliseconds."
  @spec with_poll_interval(t(), pos_integer()) :: t()
  def with_poll_interval(%__MODULE__{} = queue, milliseconds)
      when is_integer(milliseconds) and milliseconds > 0,
      do: %{queue | poll_interval: milliseconds}

  @doc "Set the maximum number of jobs checked out per poll."
  @spec with_batch_size(t(), pos_integer()) :: t()
  def with_batch_size(%__MODULE__{} = queue, batch_size)
      when is_integer(batch_size) and batch_size > 0,
      do: %{queue | batch_size: batch_size}
end
