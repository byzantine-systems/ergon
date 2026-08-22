-module(ergon_fsm).
-moduledoc """
The job lifecycle state machine, as a pure function.

`transition/2` computes the next state from a job and an event with no database
access, which makes the transition rules exhaustively testable and keeps the
authoritative definition of "what may follow what" in one place.
`ergon_db:apply_outcome/2` persists the result; the `jobs_transition_guard`
trigger enforces the same rules in the database as defense in depth, for callers
that bypass this module entirely.

## What this module does not do

The **attempt increment on checkout is not here.** `transition(Job, fetched)`
exists and is correct, but the worker path never calls it: `checkout.sql` marks
the job `executing` and increments `attempt` in the same statement that locks it,
because doing those together under `FOR UPDATE SKIP LOCKED` is what makes
checkout atomic across concurrent workers. Routing checkout through this module
would count every attempt twice.

**Retry backoff is not here either.** The delay is computed by
`ergon.retry_backoff`, a database function `apply_outcome.sql` calls: a capped
exponential in the attempt count with jitter drawn under it. This module decides
only *whether* to retry, which is what keeps it pure. Deciding *when* could not
be, since the answer is deliberately random.
""".

-include_lib("ergon/include/ergon.hrl").

-export([transition/2]).

-export_type([fsm_event/0, fsm_outcome/0, invalid_transition/0]).

-doc """
Compute the outcome of applying `Event` to `Job`.

The retry decision lives here: an errored job goes back to `available` while
attempts remain, and to `failed` once they are exhausted.
""".
-spec transition(job(), fsm_event()) -> {ok, fsm_outcome()} | {error, invalid_transition()}.
transition(#{state := State} = Job, Event) ->
    #{attempt := Attempt, max_attempts := MaxAttempts, last_error := LastError} = Job,
    case {Event, State} of
        %% A job is only fetched out of the available pool, and doing so consumes
        %% an attempt.
        {fetched, available} ->
            {ok, outcome(executing, Attempt + 1, LastError)};
        %% Only a running job can succeed.
        {succeeded, executing} ->
            {ok, outcome(completed, Attempt, null)};
        %% A running job that errors retries until its attempts are spent.
        {{errored, Reason}, executing} when Attempt >= MaxAttempts ->
            {ok, outcome(failed, Attempt, Reason)};
        {{errored, Reason}, executing} ->
            {ok, outcome(available, Attempt, Reason)};
        %% A job may be cancelled up until it reaches a terminal state.
        {cancelled, Cancellable} when Cancellable =:= available; Cancellable =:= executing ->
            {ok, outcome(discarded, Attempt, LastError)};
        {_Event, _From} ->
            {error, #{from => State, event => Event}}
    end.

outcome(State, Attempt, LastError) ->
    #{state => State, attempt => Attempt, last_error => LastError}.
