-module(ergon_job).
-moduledoc """
A fully materialised job row, plus the helpers that translate between the `state`
text column and the atoms the state machine speaks.

`job()` is the shared vocabulary of the whole library: `ergon_db`, `ergon_fsm`,
`ergon_worker`, and the `ergon` facade all pass these maps around, and a host's
handler receives one. Build one only through `from_row/1`, which decodes a row
projected with `column_list/0`.

## Payload

The column is `jsonb`, but every job-returning statement projects it as
`payload::text`, so the driver hands back a binary. `from_row/1` decodes that
into a term, mirroring the encode on the way in, so a handler sees the same shape
that was enqueued rather than having to parse JSON itself.

## Timestamps

`scheduled_at` and `inserted_at` are whatever `pg_types` decodes a `timestamptz`
into: a `{Date, Time}` tuple in UTC whose seconds field carries sub-second
precision as a float. There is no integer-epoch alternative; see `pg_timestamp()`
in `ergon.hrl`.
""".

-include_lib("ergon/include/ergon.hrl").

%% The two nominals, declared here and only here. Nominal identity is
%% per-module, so putting these in ergon.hrl would mint an incompatible pair in
%% every module that includes it. Every other module refers to them as
%% `ergon_job:job_id()` and `ergon_job:attempt()`.
%%
%% Both are non-negative integers travelling as positional arguments, which is
%% exactly where they get swapped; nominal typing makes dialyzer reject that
%% while still accepting plain integer literals.
-nominal job_id() :: non_neg_integer().
-nominal attempt() :: non_neg_integer().

-export([
    from_row/1,
    column_list/0,
    state_to_binary/1,
    state_from_binary/1
]).

-export_type([job/0, job_id/0, job_state/0, attempt/0]).

%% ---------------
%% Decoding
%% ---------------

-doc """
The column list, in order, that every job-returning statement must project so
`from_row/1` lines up with the positional row the driver returns.

This is a contract, not a convenience: the statements in `priv/queries/` spell
the projection out literally, and if one of them drifts from this list the
mismatch surfaces as a wrong field rather than a crash. It is worth an assertion
in the test suite.
""".
-spec column_list() -> binary().
column_list() ->
    <<
        "id, queue, worker, payload::text AS payload, state, fingerprint, "
        "attempt, max_attempts, last_error, scheduled_at, inserted_at"
    >>.

-doc """
Decode a row projected with `column_list/0` into a `job()`.

Rows arrive as **tuples**. The driver's own `row()` type says `list() | map()`,
but `pgo_protocol` builds each row with `list_to_tuple/1` unless
`return_rows_as_maps` is set, and it is not: positional decoding against this
module's single clause is what makes a projection that has drifted from
`column_list/0` fail loudly instead of filling the wrong fields.
""".
-spec from_row(tuple()) -> job().
from_row({
    Id,
    Queue,
    Worker,
    Payload,
    State,
    Fingerprint,
    Attempt,
    MaxAttempts,
    LastError,
    ScheduledAt,
    InsertedAt
}) ->
    #{
        id => Id,
        queue => Queue,
        worker => Worker,
        payload => json:decode(Payload),
        state => state_from_binary_or_fail(State),
        fingerprint => Fingerprint,
        attempt => Attempt,
        max_attempts => MaxAttempts,
        last_error => LastError,
        scheduled_at => ScheduledAt,
        inserted_at => InsertedAt
    }.

%% ---------------
%% States
%% ---------------

-doc "The text form of a job state, as stored in the database.".
-spec state_to_binary(job_state()) -> binary().
state_to_binary(available) -> ~"available";
state_to_binary(executing) -> ~"executing";
state_to_binary(completed) -> ~"completed";
state_to_binary(failed) -> ~"failed";
state_to_binary(discarded) -> ~"discarded".

-doc "Parse a state string, returning `error` for a value outside the domain.".
-spec state_from_binary(binary()) -> {ok, job_state()} | error.
state_from_binary(~"available") -> {ok, available};
state_from_binary(~"executing") -> {ok, executing};
state_from_binary(~"completed") -> {ok, completed};
state_from_binary(~"failed") -> {ok, failed};
state_from_binary(~"discarded") -> {ok, discarded};
state_from_binary(_Other) -> error.

%% A state outside the `ergon.job_state` domain cannot come out of the database,
%% so reaching this is a schema/decoder disagreement rather than bad input.
state_from_binary_or_fail(Raw) ->
    case state_from_binary(Raw) of
        {ok, State} -> State;
        error -> erlang:error({unknown_job_state, Raw})
    end.
