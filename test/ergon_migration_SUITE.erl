-module(ergon_migration_SUITE).
-moduledoc """
The DDL generators, checked by executing what they emit rather than by matching
strings.

String assertions are kept to the few places where the *shape* is the contract
and executing it would not prove anything: the ordering of statements, and the
`ON DELETE CASCADE` that distinguishes the two edge-table variants. Everything
else is fed to PostgreSQL, which is a far better judge of whether the DDL is
right than a regular expression is.

PostgreSQL DDL is transactional, so all of it rolls back with the fixture and
none of these tables outlive their case.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    bitemporal_table_is_created_and_versions/1,
    bitemporal_statements_are_ordered/1,
    history_twin_excludes_indexes_and_generated_columns/1,
    versioning_trigger_attaches_to_an_existing_table/1,
    vertex_table_is_created/1,
    vertex_table_can_reference_a_parent/1,
    edge_table_is_created/1,
    edge_table_cascades_the_destination_by_default/1,
    edge_table_cascade_source_option/1,
    partitioned_table_creates_the_horizon/1,
    script_joins_statements/1,
    generators_reject_bad_identifiers/1
]).

all() ->
    [
        bitemporal_table_is_created_and_versions,
        bitemporal_statements_are_ordered,
        history_twin_excludes_indexes_and_generated_columns,
        versioning_trigger_attaches_to_an_existing_table,
        vertex_table_is_created,
        vertex_table_can_reference_a_parent,
        edge_table_is_created,
        edge_table_cascades_the_destination_by_default,
        edge_table_cascade_source_option,
        partitioned_table_creates_the_horizon,
        script_joins_statements,
        generators_reject_bad_identifiers
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    ok = ergon_test_db:sandbox(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = ergon_test_db:rollback().

%% ---------------
%% Bi-temporal tables
%% ---------------

%% The claim that matters: `ergon.temporal_versioning()` is column-agnostic and
%% serves a host table it has never seen, so a host defines no function of its
%% own. Executing the DDL and then mutating a row is the only way to show it.
bitemporal_table_is_created_and_versions(_Config) ->
    T = ergon_test_db:unique(~"asset"),
    run(ergon_migration:bitemporal_table_sql(T, ~"name text NOT NULL")),

    ?assertEqual(T, regclass(T)),
    ?assertEqual(<<T/binary, "_history">>, regclass(<<T/binary, "_history">>)),

    _ = ergon_test_db:query(["INSERT INTO ", T, " (name) VALUES ('alpha')"]),
    _ = ergon_test_db:query(["UPDATE ", T, " SET name = 'beta'"]),

    ?assertEqual(~"beta", ergon_test_db:scalar(["SELECT name FROM ", T])),
    ?assertEqual(~"alpha", ergon_test_db:scalar(["SELECT name FROM ", T, "_history"])),
    %% And the archived window is closed and non-empty, the same invariant
    %% ergon.jobs holds.
    ?assertEqual(
        true,
        ergon_test_db:scalar([
            "SELECT bool_and(upper(system_time) <> 'infinity'"
            " AND lower(system_time) < upper(system_time)) FROM ",
            T,
            "_history"
        ])
    ).

%% Order is the contract: the sequence must precede the table so its DEFAULT
%% nextval resolves at CREATE time, and the trigger must come last.
bitemporal_statements_are_ordered(_Config) ->
    [Seq, Table, Owned, Twin, Index, Trigger] =
        [flat(S) || S <- ergon_migration:bitemporal_table_sql(~"widgets", ~"name text")],

    ?assert(starts_with(Seq, ~"CREATE SEQUENCE widgets_id_seq")),
    ?assert(contains(Table, ~"CREATE TABLE widgets")),
    ?assert(contains(Table, ~"PRIMARY KEY (id, valid_time WITHOUT OVERLAPS)")),
    ?assert(contains(Owned, ~"OWNED BY")),
    ?assert(contains(Twin, ~"CREATE TABLE widgets_history (LIKE widgets")),
    ?assert(contains(Index, ~"USING gist (id, system_time)")),
    ?assert(contains(Trigger, ~"CREATE TRIGGER widgets_versioning_trigger")),
    ?assert(contains(Trigger, ~"EXECUTE FUNCTION ergon.temporal_versioning()")).

%% History is an append-only log the trigger writes verbatim. A copied generated
%% column would refuse the write, and a copied temporal key would reject the very
%% overlaps history exists to record.
history_twin_excludes_indexes_and_generated_columns(_Config) ->
    Twin = flat(hd(ergon_migration:history_twin_sql(~"widgets"))),
    ?assert(contains(Twin, ~"INCLUDING DEFAULTS")),
    ?assert(contains(Twin, ~"INCLUDING CONSTRAINTS")),
    ?assertNot(contains(Twin, ~"INCLUDING INDEXES")),
    ?assertNot(contains(Twin, ~"INCLUDING GENERATED")),
    ?assertNot(contains(Twin, ~"INCLUDING ALL")).

versioning_trigger_attaches_to_an_existing_table(_Config) ->
    T = ergon_test_db:unique(~"standalone"),
    _ = ergon_test_db:query([
        "CREATE TABLE ",
        T,
        " (id bigint, name text,"
        " system_time tstzrange NOT NULL DEFAULT tstzrange(statement_timestamp(), 'infinity', '[)'))"
    ]),
    run(ergon_migration:history_twin_sql(T)),
    run([ergon_migration:versioning_trigger_sql(T)]),

    _ = ergon_test_db:query(["INSERT INTO ", T, " (id, name) VALUES (1, 'x')"]),
    _ = ergon_test_db:query(["UPDATE ", T, " SET name = 'y'"]),
    ?assertEqual(1, ergon_test_db:scalar(["SELECT count(*)::int FROM ", T, "_history"])).

%% ---------------
%% Property graph element tables
%% ---------------

vertex_table_is_created(_Config) ->
    V = ergon_test_db:unique(~"vertex"),
    run(ergon_migration:vertex_table_sql(V)),
    ?assertEqual(V, regclass(V)).

vertex_table_can_reference_a_parent(_Config) ->
    Parent = ergon_test_db:unique(~"hub"),
    _ = ergon_test_db:query(["CREATE TABLE ", Parent, " (id bigint PRIMARY KEY)"]),

    V = ergon_test_db:unique(~"hub_vertex"),
    run(ergon_migration:vertex_table_sql(V, #{references => {~"id", Parent}})),

    _ = ergon_test_db:query(["INSERT INTO ", Parent, " VALUES (1)"]),
    _ = ergon_test_db:query(["INSERT INTO ", V, " VALUES (1)"]),
    %% The FK is real: an unparented vertex is refused.
    ?assertMatch({error, _}, ergon_repo:query([~"INSERT INTO ", V, ~" VALUES (2)"], [])).

edge_table_is_created(_Config) ->
    {V, E} = graph_pair(#{}),
    ?assertEqual(E, regclass(E)),
    ?assertEqual(<<E/binary, "_history">>, regclass(<<E/binary, "_history">>)),

    _ = ergon_test_db:query(["INSERT INTO ", V, " (id) VALUES (1), (2)"]),
    _ = ergon_test_db:query(["INSERT INTO ", E, " (from_id, to_id) VALUES (1, 2)"]),
    ?assertEqual(1, ergon_test_db:scalar(["SELECT count(*)::int FROM ", E])).

%% The default asymmetry: deleting a destination vertex removes the edge,
%% deleting a source vertex is refused.
edge_table_cascades_the_destination_by_default(_Config) ->
    {V, E} = graph_pair(#{}),
    _ = ergon_test_db:query(["INSERT INTO ", V, " (id) VALUES (1), (2)"]),
    _ = ergon_test_db:query(["INSERT INTO ", E, " (from_id, to_id) VALUES (1, 2)"]),

    _ = ergon_test_db:query(["DELETE FROM ", V, " WHERE id = 2"]),
    ?assertEqual(0, ergon_test_db:scalar(["SELECT count(*)::int FROM ", E])).

edge_table_cascade_source_option(_Config) ->
    Default = flat(
        hd(
            ergon_migration:edge_table_sql(
                ~"contains", {~"parent_id", ~"parents"}, {~"child_id", ~"children"}
            )
        )
    ),
    ?assert(contains(Default, ~"parent_id bigint NOT NULL REFERENCES parents (id),")),
    ?assert(
        contains(Default, ~"child_id bigint NOT NULL REFERENCES children (id) ON DELETE CASCADE")
    ),

    Cascading = flat(
        hd(
            ergon_migration:edge_table_sql(
                ~"contains",
                {~"parent_id", ~"parents"},
                {~"child_id", ~"children"},
                #{cascade_source => true}
            )
        )
    ),
    ?assert(
        contains(Cascading, ~"parent_id bigint NOT NULL REFERENCES parents (id) ON DELETE CASCADE")
    ).

%% ---------------
%% Partitions
%% ---------------

%% The parent table is deliberately not generated, because the column list is
%% the host's. What is generated is the manage function plus the initial call.
partitioned_table_creates_the_horizon(_Config) ->
    T = ergon_test_db:unique(~"telemetry"),
    _ = ergon_test_db:query([
        "CREATE TABLE ",
        T,
        " (id bigint, recorded_at timestamptz NOT NULL) PARTITION BY RANGE (recorded_at)"
    ]),
    run(ergon_migration:partitioned_table_sql(T, ~"recorded_at")),

    %% This month plus the default two ahead.
    ?assert(partition_count(T) >= 3),

    %% Idempotent: a second call creates nothing, which is what makes it safe as
    %% a weekly cron job and as a boot check.
    ?assertEqual(0, ergon_test_db:scalar(["SELECT auto_manage_partitions_", T, "()"])).

%% ---------------
%% Assembly and safety
%% ---------------

script_joins_statements(_Config) ->
    SQL = ergon_migration:script([~"SELECT 1", ~"SELECT 2"]),
    ?assertEqual(~"SELECT 1;\n\nSELECT 2;\n\n", SQL).

%% Table and column names are interpolated, since neither can be a bind
%% parameter, so every entry point guards them.
generators_reject_bad_identifiers(_Config) ->
    Bad = ~"evil; DROP TABLE x --",
    ?assertError(invalid_identifier, ergon_migration:bitemporal_table_sql(Bad, ~"a text")),
    ?assertError(invalid_identifier, ergon_migration:vertex_table_sql(Bad)),
    ?assertError(invalid_identifier, ergon_migration:partitioned_table_sql(Bad, ~"at")),
    ?assertError(invalid_identifier, ergon_migration:versioning_trigger_sql(~"1leading_digit")),
    ?assertError(
        invalid_identifier,
        ergon_migration:edge_table_sql(~"ok", {Bad, ~"a"}, {~"b", ~"c"})
    ).

%% ---------------
%% Helpers
%% ---------------

run(Statements) ->
    [{ok, _} = ergon_repo:query(S, []) || S <- Statements],
    ok.

graph_pair(Opts) ->
    V = ergon_test_db:unique(~"gv"),
    run(ergon_migration:vertex_table_sql(V)),
    E = ergon_test_db:unique(~"ge"),
    run(ergon_migration:edge_table_sql(E, {~"from_id", V}, {~"to_id", V}, Opts)),
    {V, E}.

regclass(Name) ->
    ergon_test_db:scalar("SELECT to_regclass($1)::text", [Name]).

partition_count(Table) ->
    ergon_test_db:scalar(
        "SELECT count(*)::int FROM pg_inherits i "
        "JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = $1",
        [Table]
    ).

flat(IoData) -> iolist_to_binary(IoData).

contains(Haystack, Needle) -> binary:match(Haystack, Needle) =/= nomatch.

starts_with(Binary, Prefix) ->
    binary:longest_common_prefix([Binary, Prefix]) =:= byte_size(Prefix).
