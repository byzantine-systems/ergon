-module(ergon_cron_SUITE).
-moduledoc """
The guarded pg_cron wrappers, in three layers.

1. **Generation.** The emitted SQL carries the extension guard and quotes its
   arguments. No database needed.

2. **The no-op path**, against the test database, which has no pg_cron by design.
   The `DO` block's `IF` short-circuits and `cron.schedule` is never reached.
   This is the contract that lets one migration set run in dev and test, so
   running the generated SQL here and getting no error *is* the assertion.

3. **The active path**, against the development database, which does have
   pg_cron. Skipped unless `ERGON_TEST_CRON=1`, because it mutates `cron.job`
   rows in a database a developer is using. Each case names its own job and
   removes it afterwards.

Escaping is not a nicety here: the thing being scheduled is itself SQL and very
often contains quoted literals, as every script under `priv/migrations/cron`
does.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2, init_per_testcase/2, end_per_testcase/2]).
-export([
    schedule_sql_wraps_the_extension_guard/1,
    schedule_sql_escapes_quotes/1,
    unschedule_sql_wraps_the_extension_guard/1,
    unschedule_sql_guards_on_job_existence/1,
    unschedule_sql_escapes_quotes/1,
    dollar_tag_is_namespaced/1,
    schedule_is_a_noop_without_pg_cron/1,
    unschedule_is_a_noop_without_pg_cron/1,
    scheduled_is_empty_without_pg_cron/1,
    schedule_twice_leaves_one_row/1,
    schedule_updates_in_place/1,
    unschedule_removes_the_job/1
]).

all() -> [{group, generation}, {group, without_pg_cron}, {group, with_pg_cron}].

groups() ->
    [
        {generation, [], [
            schedule_sql_wraps_the_extension_guard,
            schedule_sql_escapes_quotes,
            unschedule_sql_wraps_the_extension_guard,
            unschedule_sql_guards_on_job_existence,
            unschedule_sql_escapes_quotes,
            dollar_tag_is_namespaced
        ]},
        {without_pg_cron, [], [
            schedule_is_a_noop_without_pg_cron,
            unschedule_is_a_noop_without_pg_cron,
            scheduled_is_empty_without_pg_cron
        ]},
        {with_pg_cron, [], [
            schedule_twice_leaves_one_row,
            schedule_updates_in_place,
            unschedule_removes_the_job
        ]}
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

%% The active group needs a database Ergon does not otherwise touch, so it opts
%% in explicitly rather than being inferred from pg_cron happening to exist.
init_per_group(with_pg_cron, Config) ->
    case os:getenv("ERGON_TEST_CRON") of
        "1" -> Config;
        _ -> {skip, "set ERGON_TEST_CRON=1 to run against the dev database's cron.job"}
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Case, Config) ->
    case lists:member(Case, dev_cases()) of
        true ->
            [{job_name, ergon_test_db:unique(~"ergon_cron_test")} | Config];
        false ->
            ok = ergon_test_db:sandbox(),
            Config
    end.

end_per_testcase(Case, Config) ->
    case lists:member(Case, dev_cases()) of
        true ->
            %% Best effort: unschedule_sql guards on job existence, so a missing
            %% row is a silent no-op.
            _ = dev_query(ergon_cron:unschedule_sql(?config(job_name, Config))),
            ok;
        false ->
            ok = ergon_test_db:rollback()
    end.

dev_cases() ->
    [schedule_twice_leaves_one_row, schedule_updates_in_place, unschedule_removes_the_job].

%% ---------------
%% Generation
%% ---------------

schedule_sql_wraps_the_extension_guard(_Config) ->
    SQL = flat(ergon_cron:schedule_sql(~"hourly", ~"0 * * * *", ~"SELECT 1")),
    ?assert(contains(SQL, ~"IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron')")),
    ?assert(contains(SQL, ~"cron.schedule(")),
    ?assert(contains(SQL, ~"'hourly'")),
    ?assert(contains(SQL, ~"'0 * * * *'")),
    ?assert(contains(SQL, ~"'SELECT 1'")).

schedule_sql_escapes_quotes(_Config) ->
    SQL = flat(ergon_cron:schedule_sql(~"with 'quote'", ~"* * * * *", ~"SELECT 'hello'")),
    ?assert(contains(SQL, ~"'with ''quote'''")),
    ?assert(contains(SQL, ~"'SELECT ''hello'''")).

unschedule_sql_wraps_the_extension_guard(_Config) ->
    SQL = flat(ergon_cron:unschedule_sql(~"hourly")),
    ?assert(contains(SQL, ~"IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron')")),
    ?assert(contains(SQL, ~"cron.unschedule(")).

unschedule_sql_guards_on_job_existence(_Config) ->
    SQL = flat(ergon_cron:unschedule_sql(~"hourly")),
    ?assert(contains(SQL, ~"IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'hourly') THEN")).

unschedule_sql_escapes_quotes(_Config) ->
    SQL = flat(ergon_cron:unschedule_sql(~"with 'quote'")),
    ?assert(contains(SQL, ~"'with ''quote'''")).

%% Tagged `$ergon_cron$` rather than a bare `$$`. These statements are generated
%% to be embedded in a host's migration, and a host wrapping them in a `DO $$ ...
%% $$` block of its own would otherwise have the inner tag close the outer one.
dollar_tag_is_namespaced(_Config) ->
    SQL = flat(ergon_cron:schedule_sql(~"n", ~"* * * * *", ~"SELECT 1")),
    ?assert(contains(SQL, ~"DO $ergon_cron$")),
    ?assert(contains(SQL, ~"END $ergon_cron$")).

%% ---------------
%% Without pg_cron
%% ---------------

schedule_is_a_noop_without_pg_cron(_Config) ->
    ?assertEqual(ok, ergon_cron:schedule(~"ergon_cron_noop", ~"* * * * *", ~"SELECT 1")).

unschedule_is_a_noop_without_pg_cron(_Config) ->
    ?assertEqual(ok, ergon_cron:unschedule(~"ergon_cron_noop")).

%% Without the extension there is no cron.job relation at all, so this reports a
%% failure rather than an empty list. Worth pinning: a caller must not read an
%% error as "nothing scheduled".
scheduled_is_empty_without_pg_cron(_Config) ->
    ?assertMatch({error, _}, ergon_cron:scheduled()).

%% ---------------
%% With pg_cron, against the dev database
%% ---------------

%% cron.schedule upserts by name (pg_cron 1.6+, pinned in flake.nix), which is
%% what makes the `always`-class scripts under priv/migrations/cron safe to
%% re-apply on every migrate.
schedule_twice_leaves_one_row(Config) ->
    Name = ?config(job_name, Config),
    dev_query(ergon_cron:schedule_sql(Name, ~"* * * * *", ~"SELECT 1")),
    dev_query(ergon_cron:schedule_sql(Name, ~"* * * * *", ~"SELECT 1")),
    ?assertEqual(1, dev_job_count(Name)).

schedule_updates_in_place(Config) ->
    Name = ?config(job_name, Config),
    dev_query(ergon_cron:schedule_sql(Name, ~"* * * * *", ~"SELECT 1")),
    dev_query(ergon_cron:schedule_sql(Name, ~"*/5 * * * *", ~"SELECT 2")),

    ?assertEqual(1, dev_job_count(Name)),
    ?assertEqual(
        [{~"*/5 * * * *", ~"SELECT 2"}],
        dev_rows(
            "SELECT schedule, command FROM cron.job WHERE jobname = $1", [Name]
        )
    ).

unschedule_removes_the_job(Config) ->
    Name = ?config(job_name, Config),
    dev_query(ergon_cron:schedule_sql(Name, ~"* * * * *", ~"SELECT 1")),
    ?assertEqual(1, dev_job_count(Name)),

    dev_query(ergon_cron:unschedule_sql(Name)),
    ?assertEqual(0, dev_job_count(Name)),

    %% Idempotent: unscheduling a job that is already gone is a no-op.
    dev_query(ergon_cron:unschedule_sql(Name)).

%% ---------------
%% Helpers
%% ---------------

flat(IoData) -> iolist_to_binary(IoData).

contains(Haystack, Needle) -> binary:match(Haystack, Needle) =/= nomatch.

%% A short-lived epgsql connection to the *development* database, where pg_cron
%% lives. It is a different database from the one the suite otherwise runs
%% against, so it cannot come out of Ergon's pool.
dev_conn() ->
    {ok, Conn} = epgsql:connect(#{
        host => os:getenv("PGHOST", "127.0.0.1"),
        port => list_to_integer(os:getenv("PGPORT", "5432")),
        username => os:getenv("PGUSER", "ergon"),
        password => os:getenv("PGPASSWORD", "ergon"),
        database => os:getenv("ERGON_DEV_DATABASE", "ergon"),
        timeout => 10000
    }),
    Conn.

dev_query(SQL) ->
    Conn = dev_conn(),
    try
        {ok, [], []} = epgsql:squery(Conn, iolist_to_binary(SQL)),
        ok
    after
        epgsql:close(Conn)
    end.

dev_rows(SQL, Params) ->
    Conn = dev_conn(),
    try
        {ok, _, Rows} = epgsql:equery(Conn, SQL, Params),
        Rows
    after
        epgsql:close(Conn)
    end.

dev_job_count(Name) ->
    [{Count}] = dev_rows("SELECT count(*)::int FROM cron.job WHERE jobname = $1", [Name]),
    Count.
