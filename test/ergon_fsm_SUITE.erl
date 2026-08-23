-module(ergon_fsm_SUITE).
-moduledoc """
`ergon_fsm:transition/2` is pure and total over `{Event, Job}`, which makes it
the one part of Ergon where property-based testing is straightforwardly the right
tool rather than an affectation.

Each property generalises what an example-based test would spot-check. The
retry rule is the reason: an example suite tends to check attempt-versus-max at
one or two hand-picked pairs, which is exactly the shape of test that passes
while an off-by-one sits in the boundary.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").

-export([all/0]).
-export([
    fetched_consumes_one_attempt/1,
    succeeded_clears_last_error/1,
    errored_retries_while_attempts_remain/1,
    cancelled_discards_non_terminal/1,
    illegal_events_are_rejected/1
]).

-define(NUMTESTS, 300).

all() ->
    [
        fetched_consumes_one_attempt,
        succeeded_clears_last_error,
        errored_retries_while_attempts_remain,
        cancelled_discards_non_terminal,
        illegal_events_are_rejected
    ].

%% ---------------
%% Generators
%% ---------------

states() -> [available, executing, completed, failed, discarded].

event_kinds() -> [fetched, succeeded, cancelled, errored].

%% The transition table's shape, independent of the attempt arithmetic that each
%% legal-transition property checks on its own.
legal_shapes() ->
    [
        {fetched, available},
        {succeeded, executing},
        {cancelled, available},
        {cancelled, executing},
        {errored, executing}
    ].

%% Precomputed rather than generate-and-filter. With five states and four event
%% kinds the illegal set is three quarters of a fifteen-element domain, so
%% sampling it directly is both exact and cheaper than rejecting the legal
%% quarter over and over.
illegal_shapes() ->
    [
        {Kind, State}
     || State <- states(),
        Kind <- event_kinds(),
        not lists:member({Kind, State}, legal_shapes())
    ].

attempt() -> integer(0, 1000).

max_attempts() -> integer(1, 1000).

last_error() -> oneof([null, reason()]).

reason() -> ?LET(S, list(choose($a, $z)), list_to_binary(S)).

%% Only state, attempt, max_attempts and last_error reach transition/2. The rest
%% of the job is a fixed placeholder the state machine never reads.
job(State, Attempt, MaxAttempts, LastError) ->
    #{
        id => 1,
        queue => ~"default",
        worker => ~"test",
        payload => #{},
        state => State,
        fingerprint => ~"abc",
        attempt => Attempt,
        max_attempts => MaxAttempts,
        last_error => LastError,
        scheduled_at => {{2026, 1, 1}, {0, 0, 0}},
        inserted_at => {{2026, 1, 1}, {0, 0, 0}}
    }.

%% ---------------
%% Cases
%% ---------------

fetched_consumes_one_attempt(_Config) ->
    check(
        ?FORALL(
            {Attempt, Max, Err},
            {attempt(), max_attempts(), last_error()},
            {ok, #{state => executing, attempt => Attempt + 1, last_error => Err}} =:=
                ergon_fsm:transition(job(available, Attempt, Max, Err), fetched)
        )
    ).

succeeded_clears_last_error(_Config) ->
    check(
        ?FORALL(
            {Attempt, Max, Err},
            {attempt(), max_attempts(), last_error()},
            {ok, #{state => completed, attempt => Attempt, last_error => null}} =:=
                ergon_fsm:transition(job(executing, Attempt, Max, Err), succeeded)
        )
    ).

%% The boundary is the whole point: at Attempt =:= Max the job has spent its last
%% attempt and fails, at Attempt < Max it goes back to available for another.
errored_retries_while_attempts_remain(_Config) ->
    check(
        ?FORALL(
            {Attempt, Max, Reason},
            {attempt(), max_attempts(), reason()},
            begin
                Expected =
                    case Attempt >= Max of
                        true -> failed;
                        false -> available
                    end,
                {ok, #{state => Expected, attempt => Attempt, last_error => Reason}} =:=
                    ergon_fsm:transition(job(executing, Attempt, Max, null), {errored, Reason})
            end
        )
    ).

cancelled_discards_non_terminal(_Config) ->
    check(
        ?FORALL(
            {State, Attempt, Max, Err},
            {oneof([available, executing]), attempt(), max_attempts(), last_error()},
            {ok, #{state => discarded, attempt => Attempt, last_error => Err}} =:=
                ergon_fsm:transition(job(State, Attempt, Max, Err), cancelled)
        )
    ).

illegal_events_are_rejected(_Config) ->
    check(
        ?FORALL(
            {{Kind, State}, Attempt, Max, Err, Reason},
            {oneof(illegal_shapes()), attempt(), max_attempts(), last_error(), reason()},
            begin
                Event =
                    case Kind of
                        errored -> {errored, Reason};
                        Other -> Other
                    end,
                {error, #{from => State, event => Event}} =:=
                    ergon_fsm:transition(job(State, Attempt, Max, Err), Event)
            end
        )
    ).

%% ---------------
%% Helpers
%% ---------------

%% `to_file` sends the shrunk counterexample to CT's log rather than to a
%% standard output nobody reads, so a failure arrives with the input that caused
%% it instead of just a line number.
check(Property) ->
    ?assert(proper:quickcheck(Property, [{numtests, ?NUMTESTS}, {to_file, user}])).
