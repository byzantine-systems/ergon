defmodule Ergon.FSMTest do
  # Ergon.FSM.transition/2 is a pure, total function over {event, job.state},'
  # the textbook case for property-based testing (see the investigation this
  # replaces: the old example-based suite only ever spot-checked attempt vs
  # max_attempts at 1-vs-3 and 3-vs-3). Each property below generalizes one
  # example-based test to the full input domain instead of one fixed pair, so
  # the hand-written examples they replaced are gone rather than duplicated.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ergon.FSM
  alias Ergon.FSM.{InvalidTransition, Outcome}
  alias Ergon.Job

  @states [:available, :executing, :completed, :failed, :discarded]

  # Only state/attempt/max_attempts/last_error affect FSM.transition/2, the
  # rest of the struct is a fixed placeholder the FSM never reads.
  defp job(opts) do
    %Job{
      id: 1,
      queue: "default",
      worker: "test",
      payload: "{}",
      state: Keyword.fetch!(opts, :state),
      fingerprint: "abc",
      attempt: Keyword.fetch!(opts, :attempt),
      max_attempts: Keyword.fetch!(opts, :max_attempts),
      last_error: Keyword.get(opts, :last_error),
      scheduled_at: DateTime.from_unix!(0),
      inserted_at: DateTime.from_unix!(0)
    }
  end

  defp attempt_gen, do: integer(0..1000)
  defp max_attempts_gen, do: integer(1..1000)
  defp last_error_gen, do: one_of([constant(nil), string(:alphanumeric)])
  defp reason_gen, do: string(:alphanumeric)

  @event_kinds [:fetched, :succeeded, :cancelled, :errored]

  # The transition table's *shape*, independent of the attempt/max_attempts
  # arithmetic, which each legal-transition property above checks on its own.
  @legal_shapes [
    {:fetched, :available},
    {:succeeded, :executing},
    {:cancelled, :available},
    {:cancelled, :executing},
    {:errored, :executing}
  ]

  # Precomputed instead of generate-then-filter: with only 5 states x 4 event
  # kinds, filtering out the legal quarter is exactly the "generate a lot,
  # throw most away" anti-pattern StreamData's own FilterTooNarrowError
  # guards against for small discrete domains. Sampling directly from the
  # illegal subset is both correct and unconditionally efficient.
  @illegal_shapes for state <- @states,
                      kind <- @event_kinds,
                      {kind, state} not in @legal_shapes,
                      do: {kind, state}

  defp event_from(:errored, reason), do: {:errored, reason}
  defp event_from(kind, _reason), do: kind

  describe "transition/2, legal transitions" do
    property ":fetched moves an available job to :executing and consumes exactly one attempt" do
      check all(
              attempt <- attempt_gen(),
              max_attempts <- max_attempts_gen(),
              last_error <- last_error_gen()
            ) do
        j =
          job(
            state: :available,
            attempt: attempt,
            max_attempts: max_attempts,
            last_error: last_error
          )

        assert FSM.transition(j, :fetched) ==
                 {:ok, %Outcome{state: :executing, attempt: attempt + 1, last_error: last_error}}
      end
    end

    property ":succeeded completes an executing job, clears last_error, and does not touch attempt" do
      check all(
              attempt <- attempt_gen(),
              max_attempts <- max_attempts_gen(),
              last_error <- last_error_gen()
            ) do
        j =
          job(
            state: :executing,
            attempt: attempt,
            max_attempts: max_attempts,
            last_error: last_error
          )

        assert FSM.transition(j, :succeeded) ==
                 {:ok, %Outcome{state: :completed, attempt: attempt, last_error: nil}}
      end
    end

    property "an errored executing job retries iff attempts remain, else fails" do
      check all(
              attempt <- attempt_gen(),
              max_attempts <- max_attempts_gen(),
              reason <- reason_gen()
            ) do
        j = job(state: :executing, attempt: attempt, max_attempts: max_attempts)
        expected_state = if attempt >= max_attempts, do: :failed, else: :available

        assert FSM.transition(j, {:errored, reason}) ==
                 {:ok, %Outcome{state: expected_state, attempt: attempt, last_error: reason}}
      end
    end

    property ":cancelled discards a non-terminal job, preserving its attempt and last_error" do
      check all(
              state <- member_of([:available, :executing]),
              attempt <- attempt_gen(),
              max_attempts <- max_attempts_gen(),
              last_error <- last_error_gen()
            ) do
        j = job(state: state, attempt: attempt, max_attempts: max_attempts, last_error: last_error)

        assert FSM.transition(j, :cancelled) ==
                 {:ok, %Outcome{state: :discarded, attempt: attempt, last_error: last_error}}
      end
    end
  end

  describe "transition/2, illegal transitions" do
    property "any event not legal for the job's current state is rejected" do
      check all(
              {kind, state} <- member_of(@illegal_shapes),
              reason <- reason_gen(),
              attempt <- attempt_gen(),
              max_attempts <- max_attempts_gen(),
              last_error <- last_error_gen()
            ) do
        j = job(state: state, attempt: attempt, max_attempts: max_attempts, last_error: last_error)
        event = event_from(kind, reason)

        assert FSM.transition(j, event) == {:error, %InvalidTransition{from: state, event: event}}
      end
    end
  end
end
