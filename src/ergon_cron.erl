-module(ergon_cron).
-moduledoc """
Scheduling SQL with pg_cron, guarded so the same code runs where it is absent.

pg_cron can be `CREATE EXTENSION`'d in exactly one database per cluster, named by
`cron.database_name` in `postgresql.conf`. The development database here has it,
the test database deliberately does not, and host clusters vary. Every helper is
therefore a guarded no-op when the extension is missing, which is what lets one
migration run unchanged against both.

Idempotent through `cron.schedule`'s upsert-by-name, so scheduling the same job
twice updates it in place rather than creating a duplicate. That is exactly the
contract a re-runnable migration needs, and it is why `priv/migrations/cron/`
scripts are class `always`.

## On one cron job per thing

`ergon_migration:partitioned_table_sql/2` schedules one **weekly** job per
partitioned table, and that is fine at any table count.

Worth stating because Phase 5 went the other way for pgmq, consolidating a
per-queue tick into one. The difference is not the number of jobs, it is what
they do: a per-second tick that calls `pg_notify` is a notifying transaction per
queue per second, and every one of those takes the global notification-queue lock
at commit. A weekly job that creates partitions takes no such lock and runs
thousands of times less often. Do not consolidate this one by analogy.
""".

-export([
    schedule/3,
    unschedule/1,
    schedule_sql/3,
    unschedule_sql/1,
    scheduled/0
]).

-include_lib("ergon/include/ergon.hrl").

-doc """
Schedule `SQL` on the cron `Spec`, named `Name`.

`Spec` is standard five-field cron syntax, or one of pg_cron's extensions such as
`@weekly` or `1 second`. A no-op where pg_cron is not installed.
""".
-spec schedule(binary(), binary(), iodata()) -> ok | {error, db_error()}.
schedule(Name, Spec, SQL) when is_binary(Name), is_binary(Spec) ->
    execute(schedule_sql(Name, Spec, SQL)).

-doc "Unschedule the job named `Name`. A no-op if it or pg_cron is absent.".
-spec unschedule(binary()) -> ok | {error, db_error()}.
unschedule(Name) when is_binary(Name) ->
    execute(unschedule_sql(Name)).

-doc "Every currently scheduled job name and its schedule.".
-spec scheduled() -> {ok, [{binary(), binary()}]} | {error, db_error()}.
scheduled() ->
    maybe
        {ok, #{rows := Rows}} ?=
            ergon_repo:query(
                "SELECT jobname, schedule FROM cron.job ORDER BY jobname", []
            ),
        {ok, [{JobName, Schedule} || {JobName, Schedule} <:- Rows]}
    end.

%% ---------------
%% SQL-only forms
%% ---------------

-doc """
The statement `schedule/3` runs, as literal SQL, for embedding in a migration.
""".
-spec schedule_sql(binary(), binary(), iodata()) -> iodata().
schedule_sql(Name, Spec, SQL) when is_binary(Name), is_binary(Spec) ->
    guarded([
        "PERFORM cron.schedule(",
        literal(Name),
        ", ",
        literal(Spec),
        ", ",
        literal(iolist_to_binary(SQL)),
        ");"
    ]).

-doc "The statement `unschedule/1` runs, as literal SQL.".
-spec unschedule_sql(binary()) -> iodata().
unschedule_sql(Name) when is_binary(Name) ->
    guarded([
        "IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = ",
        literal(Name),
        ") THEN PERFORM cron.unschedule(",
        literal(Name),
        "); END IF;"
    ]).

%% ---------------
%% Helpers
%% ---------------

%% Every statement is wrapped in the same extension guard. It has to be a DO
%% block rather than a WHERE clause, because `cron.schedule` does not exist to be
%% parsed at all when the extension is absent.
%%
%% Tagged `$ergon_cron$` rather than a bare `$$`, which is the one place in Ergon
%% that departs from the bare form. These statements are generated to be embedded
%% in a host's migration, and a host wrapping them in a `DO $$ ... $$` block of
%% its own would otherwise have the inner tag close the outer one. The cost is
%% that a migraterl `variables` key named `ergon_cron` would rewrite the tag, the
%% same hazard `ergon_migrate` documents for `$$`.
guarded(Body) ->
    [
        "DO $ergon_cron$ BEGIN "
        "IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN ",
        Body,
        " END IF; END $ergon_cron$"
    ].

%% A single-quoted SQL literal, with internal quotes doubled.
%%
%% Not optional, and not merely defensive: the thing being scheduled is itself
%% SQL and very often contains quoted literals of its own, as every script under
%% priv/migrations/cron does.
literal(Value) when is_binary(Value) ->
    ["'", binary:replace(Value, ~"'", ~"''", [global]), "'"].

execute(SQL) ->
    maybe
        {ok, _} ?= ergon_repo:query(SQL, []),
        ok
    end.
