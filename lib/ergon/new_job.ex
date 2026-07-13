defmodule Ergon.NewJob do
  @moduledoc """
  The specification for a job that has not been inserted yet.

  Build one with `new/2` and the pipeline-friendly setters rather than
  constructing the struct directly, so that adding future tuning knobs never
  breaks existing call sites:

      Ergon.NewJob.new("send_email", %{to: "a@b.com"})
      |> Ergon.NewJob.on_queue("mailers")
      |> Ergon.NewJob.with_max_attempts(5)
      |> Ergon.NewJob.unique_for(60)
      |> Ergon.enqueue()
  """

  @typedoc """
  Whether Ergon should prevent duplicate jobs, and for how long.

  Modelling this as a tagged value rather than a bool plus nullable duration
  makes the "unique but no window" and "not unique but has a window" states
  unrepresentable.
  """
  @type uniqueness :: :not_unique | {:unique_for, pos_integer()}

  @enforce_keys [:queue, :worker, :payload, :max_attempts, :uniqueness]
  defstruct queue: "default",
            worker: nil,
            payload: %{},
            max_attempts: 20,
            uniqueness: :not_unique

  @type t :: %__MODULE__{
          queue: String.t(),
          worker: String.t(),
          payload: map(),
          max_attempts: pos_integer(),
          uniqueness: uniqueness()
        }

  @doc """
  Start building a job for `worker` carrying `payload` (a JSON-encodable map).
  Defaults to the `default` queue, 20 attempts, and no uniqueness.
  """
  @spec new(String.t(), map()) :: t()
  def new(worker, payload \\ %{}) when is_binary(worker) and is_map(payload) do
    %__MODULE__{
      queue: "default",
      worker: worker,
      payload: payload,
      max_attempts: 20,
      uniqueness: :not_unique
    }
  end

  @doc "Place the job on a specific queue."
  @spec on_queue(t(), String.t()) :: t()
  def on_queue(%__MODULE__{} = job, queue) when is_binary(queue),
    do: %{job | queue: queue}

  @doc "Set how many times the job may be attempted before it is marked failed."
  @spec with_max_attempts(t(), pos_integer()) :: t()
  def with_max_attempts(%__MODULE__{} = job, max_attempts)
      when is_integer(max_attempts) and max_attempts > 0,
      do: %{job | max_attempts: max_attempts}

  @doc """
  Make the job unique for the given number of seconds: enqueuing a second job
  with the same `(queue, worker, payload)` fingerprint inside that window is
  rejected by PostgreSQL's temporal unique constraint.
  """
  @spec unique_for(t(), pos_integer()) :: t()
  def unique_for(%__MODULE__{} = job, seconds) when is_integer(seconds) and seconds > 0,
    do: %{job | uniqueness: {:unique_for, seconds}}
end
