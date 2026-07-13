defmodule Ergon.FSM do
  @moduledoc """
  The job lifecycle state machine, as a pure function.

  `transition/2` computes the next state from the current job and an event with
  no database access, which makes the transition rules exhaustively testable
  and keeps the authoritative definition of "what may follow what" in one
  place. `Ergon.DB.apply_outcome/2` persists the result, and `Ergon.JobFSM`
  wraps this in an OTP `:gen_statem` process, but both defer the decision here.
  """
  alias Ergon.Job

  @typedoc """
  Something that has happened to a job and may move it to a new state.

    * `:fetched`          – a worker checked the job out to run it
    * `:succeeded`        – the handler returned successfully
    * `{:errored, why}`   – the handler returned an error carrying `why`
    * `:cancelled`        – the job was cancelled before completing
  """
  @type event :: :fetched | :succeeded | {:errored, String.t()} | :cancelled

  defmodule Outcome do
    @moduledoc "The state to persist after a transition, with its attempt count and last error."
    @enforce_keys [:state, :attempt]
    defstruct [:state, :attempt, :last_error]

    @type t :: %__MODULE__{
            state: Ergon.Job.state(),
            attempt: non_neg_integer(),
            last_error: String.t() | nil
          }
  end

  defmodule InvalidTransition do
    @moduledoc "Raised/returned when an event does not make sense for a job's current state."
    @enforce_keys [:from, :event]
    defstruct [:from, :event]

    @type t :: %__MODULE__{from: Ergon.Job.state(), event: Ergon.FSM.event()}
  end

  @doc """
  Compute the outcome of applying `event` to `job`.

  The retry decision lives here: an errored job goes back to `:available` while
  attempts remain, and to `:failed` once they are exhausted.
  """
  @spec transition(Job.t(), event()) :: {:ok, Outcome.t()} | {:error, InvalidTransition.t()}
  def transition(%Job{} = job, event) do
    case {event, job.state} do
      # A job is only fetched out of the available pool, and doing so consumes
      # an attempt.
      {:fetched, :available} ->
        {:ok, %Outcome{state: :executing, attempt: job.attempt + 1, last_error: job.last_error}}

      # Only a running job can succeed.
      {:succeeded, :executing} ->
        {:ok, %Outcome{state: :completed, attempt: job.attempt, last_error: nil}}

      # A running job that errors retries until its attempts are spent.
      {{:errored, reason}, :executing} ->
        if job.attempt >= job.max_attempts do
          {:ok, %Outcome{state: :failed, attempt: job.attempt, last_error: reason}}
        else
          {:ok, %Outcome{state: :available, attempt: job.attempt, last_error: reason}}
        end

      # A job may be cancelled up until it reaches a terminal state.
      {:cancelled, state} when state in [:available, :executing] ->
        {:ok, %Outcome{state: :discarded, attempt: job.attempt, last_error: job.last_error}}

      {event, from} ->
        {:error, %InvalidTransition{from: from, event: event}}
    end
  end
end
