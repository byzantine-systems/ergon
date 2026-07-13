defmodule Ergon.JobFSM do
  @moduledoc """
  An OTP `:gen_statem` process that coordinates a single job's lifecycle.

  The process carries the `%Ergon.Job{}` as its data and mirrors the job's
  lifecycle in its state name, giving a supervised place to track an in-flight
  job with built-in transition validation. Every decision is delegated to the
  pure `Ergon.FSM.transition/2`, so this module and `Ergon.Worker` can never
  disagree about what a legal transition is.

  Persistence is intentionally *not* done here: the state kept by this process
  is in-memory, and `Ergon.DB.apply_outcome/2` is what writes a transition to
  the database (see `Ergon.Worker`).
  """
  @behaviour :gen_statem

  alias Ergon.{FSM, Job}

  # --- Client API ------------------------------------------------------------

  @doc "Start a state machine tracking `job`, beginning in the job's current state."
  @spec start_link(Job.t()) :: :gen_statem.start_ret()
  def start_link(%Job{} = job), do: :gen_statem.start_link(__MODULE__, job, [])

  @doc "Move an available job into execution."
  @spec start_execution(:gen_statem.server_ref()) ::
          {:ok, Job.t()} | {:error, FSM.InvalidTransition.t()}
  def start_execution(pid), do: :gen_statem.call(pid, :fetched)

  @doc "Mark a running job completed."
  @spec complete(:gen_statem.server_ref()) ::
          {:ok, Job.t()} | {:error, FSM.InvalidTransition.t()}
  def complete(pid), do: :gen_statem.call(pid, :succeeded)

  @doc "Fail a running job with `reason`, and it retries until its attempts are spent."
  @spec fail(:gen_statem.server_ref(), String.t()) ::
          {:ok, Job.t()} | {:error, FSM.InvalidTransition.t()}
  def fail(pid, reason), do: :gen_statem.call(pid, {:errored, reason})

  @doc "Cancel a non-terminal job."
  @spec cancel(:gen_statem.server_ref()) ::
          {:ok, Job.t()} | {:error, FSM.InvalidTransition.t()}
  def cancel(pid), do: :gen_statem.call(pid, :cancelled)

  @doc "The job as currently held by the process."
  @spec job(:gen_statem.server_ref()) :: Job.t()
  def job(pid), do: :gen_statem.call(pid, :get_job)

  @doc "Stop the state machine."
  @spec stop(:gen_statem.server_ref()) :: :ok
  def stop(pid), do: :gen_statem.stop(pid)

  # --- gen_statem callbacks --------------------------------------------------

  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  @impl :gen_statem
  def init(%Job{} = job), do: {:ok, job.state, job}

  @impl :gen_statem
  def handle_event({:call, from}, :get_job, _state, %Job{} = job) do
    {:keep_state_and_data, [{:reply, from, job}]}
  end

  # Every lifecycle event is an `Ergon.FSM.event`, and the pure transition decides
  # whether it is legal and what the resulting state is.
  def handle_event({:call, from}, event, _state, %Job{} = job) do
    case FSM.transition(job, event) do
      {:ok, outcome} ->
        updated = %{
          job
          | state: outcome.state,
            attempt: outcome.attempt,
            last_error: outcome.last_error
        }

        {:next_state, outcome.state, updated, [{:reply, from, {:ok, updated}}]}

      {:error, invalid} ->
        {:keep_state_and_data, [{:reply, from, {:error, invalid}}]}
    end
  end
end
