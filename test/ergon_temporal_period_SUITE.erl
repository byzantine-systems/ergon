-module(ergon_temporal_period_SUITE).
-moduledoc """
`ergon_temporal_period` is pure and boundary-sensitive, which is the combination
properties are for: the interesting bugs live exactly where a probe instant
coincides with an endpoint, and a handful of examples only ever spot-checks that.

Instants are small integers mapped through `calendar`, so a probe can land
exactly on a bound rather than near it.

`from_pg_range/1` is covered from the other end, in `ergon_sql_SUITE`, where a
real `tstzrange` comes back off the wire.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").

-export([all/0]).
-export([
    new_is_closed_open/1,
    bounded_contains_matches_half_open/1,
    unbounded_lower_contains_below_upper/1,
    unbounded_upper_contains_from_lower/1,
    fully_unbounded_contains_everything/1,
    empty_contains_nothing/1,
    empty_is_marked_empty/1,
    infinity_endpoints_order_as_extremes/1
]).

-define(NUMTESTS, 300).

%% An arbitrary epoch, so the generated integers become ordinary datetimes rather
%% than year-zero ones.
-define(EPOCH, 63870000000).

all() ->
    [
        new_is_closed_open,
        bounded_contains_matches_half_open,
        unbounded_lower_contains_below_upper,
        unbounded_upper_contains_from_lower,
        fully_unbounded_contains_everything,
        empty_contains_nothing,
        empty_is_marked_empty,
        infinity_endpoints_order_as_extremes
    ].

%% ---------------
%% Generators
%% ---------------

instant(I) -> calendar:gregorian_seconds_to_datetime(?EPOCH + I).

offset() -> integer(-1000, 1000).

endpoint() -> oneof([unbounded, ?LET(I, offset(), instant(I))]).

%% ---------------
%% Cases
%% ---------------

new_is_closed_open(_Config) ->
    check(
        ?FORALL(
            {Lower, Upper},
            {endpoint(), endpoint()},
            #{
                lower => Lower,
                upper => Upper,
                lower_inclusive => true,
                upper_inclusive => false,
                empty => false
            } =:= ergon_temporal_period:new(Lower, Upper)
        )
    ).

bounded_contains_matches_half_open(_Config) ->
    check(
        ?FORALL(
            {LowerI, Span, ProbeI},
            {offset(), integer(1, 2000), integer(-1100, 1100)},
            begin
                UpperI = LowerI + Span,
                Period = ergon_temporal_period:new(instant(LowerI), instant(UpperI)),
                Expected = ProbeI >= LowerI andalso ProbeI < UpperI,
                Expected =:= ergon_temporal_period:contains(Period, instant(ProbeI))
            end
        )
    ).

unbounded_lower_contains_below_upper(_Config) ->
    check(
        ?FORALL(
            {UpperI, ProbeI},
            {offset(), integer(-1100, 1100)},
            begin
                Period = ergon_temporal_period:new(unbounded, instant(UpperI)),
                (ProbeI < UpperI) =:= ergon_temporal_period:contains(Period, instant(ProbeI))
            end
        )
    ).

unbounded_upper_contains_from_lower(_Config) ->
    check(
        ?FORALL(
            {LowerI, ProbeI},
            {offset(), integer(-1100, 1100)},
            begin
                Period = ergon_temporal_period:new(instant(LowerI), unbounded),
                (ProbeI >= LowerI) =:= ergon_temporal_period:contains(Period, instant(ProbeI))
            end
        )
    ).

fully_unbounded_contains_everything(_Config) ->
    Period = ergon_temporal_period:new(unbounded, unbounded),
    check(
        ?FORALL(
            ProbeI,
            offset(),
            ergon_temporal_period:contains(Period, instant(ProbeI))
        )
    ).

empty_contains_nothing(_Config) ->
    Period = ergon_temporal_period:empty(),
    check(
        ?FORALL(
            ProbeI,
            offset(),
            not ergon_temporal_period:contains(Period, instant(ProbeI))
        )
    ).

empty_is_marked_empty(_Config) ->
    ?assertEqual(
        #{
            lower => empty,
            upper => empty,
            lower_inclusive => false,
            upper_inclusive => false,
            empty => true
        },
        ergon_temporal_period:empty()
    ).

%% `infinity` and `-infinity` are values a timestamptz column can hold, not bound
%% markers, and they arrive as atoms. Term ordering sorts an atom after every
%% tuple, so a naive comparison would put `-infinity` in the future. This is the
%% property that says it does not.
infinity_endpoints_order_as_extremes(_Config) ->
    Everything = ergon_temporal_period:new('-infinity', infinity),
    check(
        ?FORALL(
            ProbeI,
            offset(),
            begin
                Probe = instant(ProbeI),
                ergon_temporal_period:contains(Everything, Probe) andalso
                    not ergon_temporal_period:contains(
                        ergon_temporal_period:new(infinity, infinity), Probe
                    ) andalso
                    not ergon_temporal_period:contains(
                        ergon_temporal_period:new('-infinity', '-infinity'), Probe
                    )
            end
        )
    ).

%% ---------------
%% Helpers
%% ---------------

check(Property) ->
    ?assert(proper:quickcheck(Property, [{numtests, ?NUMTESTS}, {to_file, user}])).
