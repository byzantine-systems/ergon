-module(ergon_temporal_period).
-moduledoc """
The decoded form of a PostgreSQL temporal validity period.

PostgreSQL 19 has no scalar `PERIOD` type: application-time (`valid_period`),
uniqueness (`dedup_period`), and system-time (`system_time`) are all `tstzrange`
columns. `from_pg_range/1` converts what the driver decodes such a column into,
so callers work with a named shape instead of a positional tuple.

An infinite endpoint is `unbounded`; an empty range is `empty := true` with
`empty` endpoints. Both matter to Ergon specifically: a job is live exactly when
its `valid_period` upper bound is unbounded, and a non-unique job is one whose
`dedup_period` is empty, because empty ranges never overlap and so never trip the
uniqueness constraint.

## Where this is and is not needed

No statement in `priv/queries/` projects a range column today; every
job-returning query selects scalars. This module is for hosts that query
`valid_period` or `system_time` directly, and for the bi-temporal tests. It is
pure, so nothing here needs a database.
""".

-include_lib("ergon/include/ergon.hrl").

-export([
    new/2,
    empty/0,
    contains/2,
    from_pg_range/1
]).

-export_type([temporal_period/0, period_endpoint/0]).

-doc """
A closed-open period `[Lower, Upper)`, the bound style PostgreSQL uses for
`tstzrange`. Either endpoint may be `unbounded`.
""".
-spec new(period_endpoint(), period_endpoint()) -> temporal_period().
new(Lower, Upper) ->
    #{
        lower => Lower,
        upper => Upper,
        lower_inclusive => true,
        upper_inclusive => false,
        empty => false
    }.

-doc "The empty period. Contains nothing and overlaps nothing.".
-spec empty() -> temporal_period().
empty() ->
    #{
        lower => empty,
        upper => empty,
        lower_inclusive => false,
        upper_inclusive => false,
        empty => true
    }.

-doc """
Whether `Instant` falls within the period, honouring bound inclusivity and
unbounded endpoints. An empty period contains nothing.
""".
-spec contains(temporal_period(), pg_timestamp()) -> boolean().
contains(#{empty := true}, _Instant) ->
    false;
contains(#{lower := Lower, lower_inclusive := LowerInc} = Period, Instant) ->
    #{upper := Upper, upper_inclusive := UpperInc} = Period,
    after_lower(Lower, LowerInc, Instant) andalso before_upper(Upper, UpperInc, Instant).

-doc """
Convert a range as the driver decodes it into a period.

`pg_range` is generic over its base type and answers either `empty` or
`{{From, To}, {LowerInclusive, UpperInclusive}}`, using `unbound` for an infinite
endpoint. This is the whole of the conversion: because that decoder is generic,
Ergon needs no hand-written binary codec for `tstzrange`.
""".
-spec from_pg_range(pg_range()) -> temporal_period().
from_pg_range(empty) ->
    empty();
from_pg_range({{From, To}, {LowerInclusive, UpperInclusive}}) ->
    #{
        lower => endpoint(From),
        upper => endpoint(To),
        lower_inclusive => LowerInclusive,
        upper_inclusive => UpperInclusive,
        empty => false
    }.

%% ---------------
%% Helpers
%% ---------------

endpoint(unbound) -> unbounded;
endpoint(Timestamp) -> Timestamp.

after_lower(unbounded, _Inclusive, _Instant) ->
    true;
after_lower(Lower, Inclusive, Instant) ->
    case compare(Instant, Lower) of
        gt -> true;
        eq -> Inclusive;
        lt -> false
    end.

before_upper(unbounded, _Inclusive, _Instant) ->
    true;
before_upper(Upper, Inclusive, Instant) ->
    case compare(Instant, Upper) of
        lt -> true;
        eq -> Inclusive;
        gt -> false
    end.

%% `infinity` and `-infinity` are ordinary values in a timestamptz column, not
%% bound flags, so they are compared as the extremes they represent rather than
%% falling through to term ordering, where the atom `infinity` would sort after
%% every tuple and `'-infinity'` would too.
compare(Same, Same) -> eq;
compare('-infinity', _Other) -> lt;
compare(_Other, '-infinity') -> gt;
compare(infinity, _Other) -> gt;
compare(_Other, infinity) -> lt;
compare(A, B) when A < B -> lt;
compare(_A, _B) -> gt.
