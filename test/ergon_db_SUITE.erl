-module(ergon_db_SUITE).
-moduledoc """
The job lifecycle against a live PostgreSQL 19, and the invariants the database
owns rather than Erlang.

Everything here runs inside `ergon_test_db`'s fixture transaction. That is not
only for isolation: the bi-temporal group depends on it, because the whole point
of `statement_timestamp()` over `now()` is that belief time advances *within* a
transaction, and a fixture that committed between statements would never
exercise it.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    enqueue_inserts_an_available_job/1,
    checkout_claims_a_job_exactly_once/1,
    checkout_respects_the_batch_limit/1,
    apply_outcome_records_the_transition/1,
    unique_job_is_get_or_create/1,
    unique_job_stays_checkoutable/1,
    fingerprint_is_deterministic/1,
    job_state_domain_rejects_nonsense/1,
    transition_guard_rejects_illegal_edges/1,
    retry_pushes_scheduled_at_forward/1,
    cancel_cascade_discards_live_descendants/1,
    cancel_cascade_leaves_terminal_alone/1,
    rls_isolates_tenants/1,
    insert_archives_nothing/1,
    plain_update_archives_one_closed_row/1,
    for_portion_of_archives_another/1,
    versions_tile_without_gaps/1,
    no_range_is_null_bounded/1,
    ceiling_doubles_and_then_caps/1,
    full_jitter_stays_inside_the_ceiling/1,
    equal_jitter_keeps_half_the_ceiling/1,
    jitter_varies_per_row/1,
    absurd_attempts_do_not_overflow/1
]).

-define(SPEC(Queue), ergon_new_job:on_queue(ergon_new_job:new(~"w"), Queue)).

%% The shipped defaults, so the cases below assert the curve a host gets without
%% configuring anything.
-define(BASE_MS, 1_000).
-define(CAP_MS, 100_000).

all() ->
    [{group, lifecycle}, {group, invariants}, {group, bitemporal}, {group, backoff}].

groups() ->
    [
        {lifecycle, [], [
            enqueue_inserts_an_available_job,
            checkout_claims_a_job_exactly_once,
            checkout_respects_the_batch_limit,
            apply_outcome_records_the_transition,
            unique_job_is_get_or_create,
            unique_job_stays_checkoutable,
            cancel_cascade_discards_live_descendants,
            cancel_cascade_leaves_terminal_alone
        ]},
        {invariants, [], [
            fingerprint_is_deterministic,
            job_state_domain_rejects_nonsense,
            transition_guard_rejects_illegal_edges,
            retry_pushes_scheduled_at_forward,
            rls_isolates_tenants
        ]},
        {bitemporal, [], [
            insert_archives_nothing,
            plain_update_archives_one_closed_row,
            for_portion_of_archives_another,
            versions_tile_without_gaps,
            no_range_is_null_bounded
        ]},
        {backoff, [], [
            ceiling_doubles_and_then_caps,
            full_jitter_stays_inside_the_ceiling,
            equal_jitter_keeps_half_the_ceiling,
            jitter_varies_per_row,
            absurd_attempts_do_not_overflow
        ]}
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    ok = ergon_test_db:sandbox(),
    [{queue, ergon_test_db:unique(~"q")} | Config].

end_per_testcase(_Case, _Config) ->
    ok = ergon_test_db:rollback().

%% ---------------
%% Lifecycle
%% ---------------

enqueue_inserts_an_available_job(_Config) ->
    Spec = ergon_new_job:new(~"send_email", #{~"to" => ~"a@b.com"}),
    {ok, Job} = ergon:enqueue(Spec),
    ?assertMatch(
        #{state := available, attempt := 0, queue := ~"default", worker := ~"send_email"},
        Job
    ),
    %% payload comes back decoded, not as the raw jsonb text the column projects.
    ?assertEqual(#{~"to" => ~"a@b.com"}, maps:get(payload, Job)).

checkout_claims_a_job_exactly_once(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := Id}} = ergon:enqueue(?SPEC(Queue)),

    {ok, [Claimed]} = ergon_db:checkout(Queue, 5),
    ?assertMatch(#{id := Id, state := executing, attempt := 1}, Claimed),

    %% Nothing live is left to claim.
    ?assertEqual({ok, []}, ergon_db:checkout(Queue, 5)).

%% The Phase 2 rescan fix. checkout.sql originally bounded the wrong thing, so a
%% queue holding several jobs handed back more than the batch asked for.
checkout_respects_the_batch_limit(Config) ->
    Queue = ?config(queue, Config),
    [{ok, _} = ergon:enqueue(?SPEC(Queue)) || _ <- lists:seq(1, 5)],
    {ok, Claimed} = ergon_db:checkout(Queue, 1),
    ?assertEqual(1, length(Claimed)).

apply_outcome_records_the_transition(Config) ->
    Queue = ?config(queue, Config),
    {ok, _} = ergon:enqueue(?SPEC(Queue)),
    {ok, [Claimed]} = ergon_db:checkout(Queue, 1),
    {ok, Outcome} = ergon_fsm:transition(Claimed, succeeded),
    {ok, Completed} = ergon_db:apply_outcome(maps:get(id, Claimed), Outcome),
    ?assertMatch(#{state := completed}, Completed).

%% A duplicate inside the window returns the existing job rather than an error:
%% ergon.enqueue absorbs the temporal EXCLUDE conflict.
unique_job_is_get_or_create(_Config) ->
    Spec = ergon_new_job:unique_for(
        ergon_new_job:new(~"report", #{~"day" => ~"2026-07-14"}), 3600
    ),
    {ok, #{id := First}} = ergon:enqueue(Spec),
    {ok, #{id := Second}} = ergon:enqueue(Spec),
    ?assertEqual(First, Second).

unique_job_stays_checkoutable(Config) ->
    Queue = ?config(queue, Config),
    Spec = ergon_new_job:unique_for(
        ergon_new_job:on_queue(ergon_new_job:new(~"digest"), Queue), 3600
    ),
    {ok, #{id := Id}} = ergon:enqueue(Spec),
    ?assertMatch({ok, [#{id := Id, state := executing}]}, ergon_db:checkout(Queue, 1)).

cancel_cascade_discards_live_descendants(Config) ->
    {A, B, C} = chain(?config(queue, Config)),
    {ok, Discarded} = ergon_db:cancel_cascade(A),
    ?assertEqual([A, B, C], lists:sort([Id || #{id := Id} <- Discarded])),
    ?assert(lists:all(fun(#{state := S}) -> S =:= discarded end, Discarded)).

%% A completed descendant is terminal, so the cascade must skip it while still
%% discarding the rest.
%%
%% The descendant is completed *before* it is linked, which is the only order
%% that works now: since Phase 4 a child with unfinished parents carries
%% `pending_parents > 0` and is absent from `jobs_fetch_idx` entirely, so
%% checking one out to complete it is not possible while its parent is live. The
%% Elixir original claimed all three jobs in one checkout and predates that.
cancel_cascade_leaves_terminal_alone(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := A}} = ergon:enqueue(?SPEC(Queue)),
    {ok, #{id := B}} = ergon:enqueue(?SPEC(Queue)),
    {ok, #{id := C}} = ergon:enqueue(?SPEC(Queue)),

    %% C reaches a terminal state on its own, unblocked by anything.
    {ok, Claimed} = ergon_db:checkout(Queue, 3),
    [CJob] = [J || #{id := Id} = J <- Claimed, Id =:= C],
    {ok, Outcome} = ergon_fsm:transition(CJob, succeeded),
    {ok, #{state := completed}} = ergon_db:apply_outcome(C, Outcome),

    %% Only now does it become a descendant of A.
    ok = ergon:depends_on(A, B),
    ok = ergon:depends_on(A, C),

    {ok, Discarded} = ergon_db:cancel_cascade(A),
    Ids = [Id || #{id := Id} <- Discarded],
    ?assertNot(lists:member(C, Ids)),
    ?assert(lists:member(A, Ids)),
    ?assert(lists:member(B, Ids)).

%% ---------------
%% Invariants the database owns
%% ---------------

fingerprint_is_deterministic(Config) ->
    Queue = ?config(queue, Config),
    Of = fun(Payload) ->
        {ok, #{fingerprint := F}} =
            ergon:enqueue(
                ergon_new_job:on_queue(ergon_new_job:new(~"w", Payload), Queue)
            ),
        F
    end,
    A = Of(#{~"k" => 1}),
    B = Of(#{~"k" => 1}),
    C = Of(#{~"k" => 2}),
    ?assertEqual(A, B),
    ?assertNotEqual(A, C),
    %% sha256, hex. Deterministic and unsalted: non-uniqueness comes from an
    %% empty dedup_period, never from perturbing the fingerprint.
    ?assertMatch({match, _}, re:run(A, "^[0-9a-f]{64}$")).

job_state_domain_rejects_nonsense(_Config) ->
    ?assertMatch(
        {error, _},
        ergon_repo:query(
            "INSERT INTO ergon.jobs (worker, state) VALUES ($1, $2)", [~"w", ~"banana"]
        )
    ).

%% completed -> executing is not a legal edge, and the trigger rejects it even
%% though the outcome here is hand-crafted to bypass ergon_fsm entirely.
transition_guard_rejects_illegal_edges(Config) ->
    Queue = ?config(queue, Config),
    {ok, _} = ergon:enqueue(?SPEC(Queue)),
    {ok, [Claimed]} = ergon_db:checkout(Queue, 1),
    Id = maps:get(id, Claimed),
    {ok, Outcome} = ergon_fsm:transition(Claimed, succeeded),
    {ok, #{state := completed, attempt := Attempt}} = ergon_db:apply_outcome(Id, Outcome),

    Illegal = #{state => executing, attempt => Attempt, last_error => null},
    ?assertMatch({error, _}, ergon_db:apply_outcome(Id, Illegal)).

%% Monotonicity alone would survive any formula, including a broken one, so this
%% also pins the window. The retry is the job's first, so its ceiling is exactly
%% `base` and full jitter puts it somewhere in `[now, now + base]`. `now()` is
%% frozen for the fixture transaction and is the same instant apply_outcome
%% anchored to, which is what makes both bounds exact rather than approximate.
retry_pushes_scheduled_at_forward(Config) ->
    Queue = ?config(queue, Config),
    {ok, _} = ergon:enqueue(?SPEC(Queue)),
    {ok, [Claimed]} = ergon_db:checkout(Queue, 1),
    {ok, Outcome} = ergon_fsm:transition(Claimed, {errored, ~"boom"}),
    ?assertMatch(#{state := available, attempt := 1}, Outcome),
    Id = maps:get(id, Claimed),
    {ok, Retried} = ergon_db:apply_outcome(Id, Outcome),
    ?assertMatch(#{state := available}, Retried),
    ?assert(maps:get(scheduled_at, Retried) > maps:get(scheduled_at, Claimed)),

    ?assert(
        ergon_test_db:scalar(
            "SELECT scheduled_at BETWEEN NOW() AND NOW() + MAKE_INTERVAL(secs => $2 / 1000.0)"
            "  FROM ergon.jobs_current WHERE id = $1",
            [Id, ?BASE_MS]
        )
    ).

%% The connecting role is a superuser and bypasses RLS, so isolation has to be
%% exercised under a restricted one. CREATE ROLE is transactional, unlike CREATE
%% DATABASE, so the role goes away with the fixture.
rls_isolates_tenants(_Config) ->
    Role = ergon_test_db:unique(~"rls"),
    _ = ergon_test_db:query(["CREATE ROLE ", Role, " NOLOGIN NOBYPASSRLS"]),
    _ = ergon_test_db:query(["GRANT USAGE ON SCHEMA ergon TO ", Role]),
    _ = ergon_test_db:query([
        "GRANT SELECT, INSERT ON ergon.jobs, ergon.jobs_history TO ", Role
    ]),

    _ = ergon_test_db:query(["SET LOCAL ROLE ", Role]),
    _ = ergon_test_db:query("SET LOCAL ergon.tenant = 'acme'"),
    _ = ergon_test_db:query("INSERT INTO ergon.jobs (worker) VALUES ('acme_job')"),
    ?assertEqual(1, ergon_test_db:scalar("SELECT count(*)::int FROM ergon.jobs")),

    _ = ergon_test_db:query("SET LOCAL ergon.tenant = 'globex'"),
    ?assertEqual(0, ergon_test_db:scalar("SELECT count(*)::int FROM ergon.jobs")),

    _ = ergon_test_db:query("RESET ROLE").

%% ---------------
%% Bi-temporal invariants
%% ---------------

%% An INSERT is not an UPDATE or a DELETE, so the trigger only stamps the open
%% system_time on the new row. Nothing is archived yet.
insert_archives_nothing(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := Id}} = ergon:enqueue(?SPEC(Queue)),
    ?assertEqual(true, live_upper_is_infinity(Id)),
    ?assertEqual(0, history_count(Id)).

%% checkout.sql is a plain UPDATE with no FOR PORTION OF, so the trigger archives
%% the prior available row with a closed system_time and the live row keeps its
%% open one.
plain_update_archives_one_closed_row(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := Id}} = ergon:enqueue(?SPEC(Queue)),
    {ok, [_]} = ergon_db:checkout(Queue, 1),

    ?assertEqual(1, history_count(Id)),
    ?assertEqual(true, all_history_windows_closed_and_nonempty(Id)).

%% Two transitions, two archived rows, and the second is what the timestamp
%% choice is for: with a transaction-frozen now() the second window would close
%% at its own lower bound and be empty, therefore invisible to every as-of query.
for_portion_of_archives_another(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := Id}} = ergon:enqueue(?SPEC(Queue)),
    {ok, [Claimed]} = ergon_db:checkout(Queue, 1),
    {ok, Outcome} = ergon_fsm:transition(Claimed, succeeded),
    {ok, _} = ergon_db:apply_outcome(Id, Outcome),

    ?assertEqual(2, history_count(Id)),
    ?assertEqual(true, all_history_windows_closed_and_nonempty(Id)).

%% Belief time has to tile: each archived window ends exactly where the next
%% begins, and the last one ends exactly where the live row's begins. Two
%% separate calls to a per-call clock would leave a gap between them, and an
%% as-of query landing in that gap would find no version of the job at all.
versions_tile_without_gaps(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := Id}} = ergon:enqueue(?SPEC(Queue)),
    {ok, [Claimed]} = ergon_db:checkout(Queue, 1),
    {ok, Outcome} = ergon_fsm:transition(Claimed, succeeded),
    {ok, _} = ergon_db:apply_outcome(Id, Outcome),

    %% Archived windows meet each other.
    ?assertEqual(
        true,
        ergon_test_db:scalar(
            "SELECT bool_and(prev_upper = lower(system_time)) FROM ("
            "  SELECT system_time,"
            "         coalesce(lag(upper(system_time)) OVER (ORDER BY lower(system_time)),"
            "                  lower(system_time)) AS prev_upper"
            "  FROM ergon.jobs_history WHERE id = $1"
            ") s",
            [Id]
        )
    ),

    %% And the last archived window meets the live row.
    ?assertEqual(
        true,
        ergon_test_db:scalar(
            "SELECT max(upper(h.system_time)) = min(lower(j.system_time)) "
            "FROM ergon.jobs_history h, ergon.jobs j "
            "WHERE h.id = j.id AND j.id = $1 AND upper(j.valid_period) = 'infinity'",
            [Id]
        )
    ).

%% Ergon writes the literal 'infinity', never a NULL bound. The two are different
%% values, `upper_inf()` separates them, and a table mixing them cannot be
%% queried consistently for liveness.
no_range_is_null_bounded(Config) ->
    Queue = ?config(queue, Config),
    {ok, #{id := Id}} = ergon:enqueue(?SPEC(Queue)),
    {ok, [_]} = ergon_db:checkout(Queue, 1),

    ?assertEqual(
        false,
        ergon_test_db:scalar(
            "SELECT bool_or(upper_inf(system_time) OR upper_inf(valid_period)) "
            "FROM ergon.jobs WHERE id = $1",
            [Id]
        )
    ),
    ?assertEqual(
        false,
        ergon_test_db:scalar(
            "SELECT coalesce(bool_or(upper_inf(valid_period)), false) "
            "FROM ergon.jobs_history WHERE id = $1",
            [Id]
        )
    ).

%% ---------------
%% Retry backoff
%% ---------------
%%
%% `ergon.retry_backoff` is exercised directly rather than through a retried job,
%% because a job carries one attempt at a time and the curve only means anything
%% across the whole range. `retry_pushes_scheduled_at_forward/1` covers the wiring.
%%
%% Every ceiling here is restated independently, as `least(cap, base * 2^(a-1))`
%% rather than the shift the function uses, so a mistake in one formulation does
%% not cancel out against the same mistake in the other.

%% `none` is the article's unjittered baseline, and the only strategy that can be
%% asserted to an exact value. It is what pins the curve: attempt 1 at the base,
%% doubling, flat at the cap from attempt 8 on.
ceiling_doubles_and_then_caps(_Config) ->
    ?assertEqual(
        [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 100_000, 100_000, 100_000],
        [delay_ms(Attempt, ~"none") || Attempt <- lists:seq(1, 10)]
    ),
    %% attempt 0 is unreachable through apply_outcome but permitted by the
    %% jobs_attempt_bounds CHECK, and must not produce half a base or a negative.
    ?assertEqual(1_000, delay_ms(0, ~"none")).

full_jitter_stays_inside_the_ceiling(_Config) ->
    ?assertEqual(0, out_of_bounds(~"full_jitter", "0")).

%% The half is the whole difference between this strategy and full jitter: it
%% trades some of the dispersion for a guaranteed floor.
equal_jitter_keeps_half_the_ceiling(_Config) ->
    ?assertEqual(0, out_of_bounds(~"equal_jitter", "ceiling / 2")).

%% The assertion that catches a revert. Every case above would still pass if the
%% jitter were dropped and the ceiling returned unchanged, because the ceiling is
%% itself a legal draw; only this one notices.
%%
%% Per row, not per call, which is the part worth stating: 200 draws in a single
%% statement, all distinct. Note it does not test the VOLATILE marking. Marking
%% the function STABLE and re-running leaves this passing, because PostgreSQL
%% does not memoise STABLE calls. The marking has to be read rather than
%% asserted.
jitter_varies_per_row(_Config) ->
    ?assert(distinct_draws(~"full_jitter") > 100),
    ?assert(distinct_draws(~"equal_jitter") > 100),
    ?assertEqual(1, distinct_draws(~"none")).

%% The exponent is clamped, so no max_attempts a host can set reaches an
%% overflow. Without the clamp this raises rather than returning the cap.
absurd_attempts_do_not_overflow(_Config) ->
    ?assertEqual(?CAP_MS, delay_ms(1_000_000, ~"none")),
    ?assertEqual(0, out_of_bounds(~"full_jitter", "0")).

%% ---------------
%% Helpers
%% ---------------

delay_ms(Attempt, Strategy) ->
    round(
        ergon_test_db:scalar(
            "SELECT EXTRACT(epoch FROM ergon.retry_backoff($1, $2, $3, $4)) * 1000",
            [Attempt, ?BASE_MS, ?CAP_MS, Strategy]
        )
    ).

%% Draws that fall outside `[Floor, ceiling]`, over 100 samples at each attempt
%% from 1 to 25, in one round trip. `Floor` is SQL rather than a parameter
%% because it is written in terms of `ceiling`.
out_of_bounds(Strategy, Floor) ->
    ergon_test_db:scalar(
        [
            "SELECT count(*)::int FROM ("
            "  SELECT EXTRACT(epoch FROM ergon.retry_backoff(a, $1, $2, $3)) * 1000 AS ms,"
            "         least($2::numeric, $1::numeric * 2 ^ greatest(a - 1, 0)) AS ceiling"
            "    FROM generate_series(1, 25) AS a, generate_series(1, 100) AS n"
            ") AS draws WHERE ms NOT BETWEEN ",
            Floor,
            " AND ceiling"
        ],
        [?BASE_MS, ?CAP_MS, Strategy]
    ).

%% Distinct delays across 200 draws in a single statement, at an attempt well
%% short of the cap so the range is wide enough for collisions to be negligible.
distinct_draws(Strategy) ->
    ergon_test_db:scalar(
        "SELECT count(DISTINCT ergon.retry_backoff(6, $1, $2, $3))::int "
        "FROM generate_series(1, 200)",
        [?BASE_MS, ?CAP_MS, Strategy]
    ).

%% a -> b -> c, all on one queue.
chain(Queue) ->
    {ok, #{id := A}} = ergon:enqueue(?SPEC(Queue)),
    {ok, #{id := B}} = ergon:enqueue(?SPEC(Queue)),
    {ok, #{id := C}} = ergon:enqueue(?SPEC(Queue)),
    ok = ergon:depends_on(A, B),
    ok = ergon:depends_on(B, C),
    {A, B, C}.

history_count(Id) ->
    ergon_test_db:scalar("SELECT count(*)::int FROM ergon.jobs_history WHERE id = $1", [Id]).

live_upper_is_infinity(Id) ->
    ergon_test_db:scalar(
        "SELECT upper(system_time) = 'infinity' FROM ergon.jobs WHERE id = $1", [Id]
    ).

all_history_windows_closed_and_nonempty(Id) ->
    ergon_test_db:scalar(
        "SELECT bool_and(upper(system_time) <> 'infinity' "
        "                AND lower(system_time) < upper(system_time)) "
        "FROM ergon.jobs_history WHERE id = $1",
        [Id]
    ).
