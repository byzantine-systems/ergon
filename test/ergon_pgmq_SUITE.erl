-module(ergon_pgmq_SUITE).
-moduledoc """
The pgmq wrappers against a real pgmq installation.

Every case owns a queue named for itself. That is not redundant with the fixture
transaction: `pgmq.create` writes a row in pgmq's own metadata table and creates
`pgmq.q_<name>`, both cluster-visible, so two cases sharing a name would contend
whatever the rows do. The name is unique per run for the same reason a previous
failed run must not be able to donate a queue to this one: `pgmq.create` is
idempotent, so a leftover queue is silently reused and its archive counts come
back wrong.

The groups beyond `basics` are Phase 5 additions with no Elixir ancestor: FIFO
message groups, topics, and server-side long polling.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    read_hides_behind_the_visibility_timeout/1,
    expired_lease_redelivers_with_higher_read_ct/1,
    archive_acks_a_batch/1,
    archive_of_an_unknown_id_is_empty/1,
    archive_of_nothing_touches_the_database/1,
    metrics_reports_queue_length/1,
    release_leases_frees_stranded_messages/1,
    release_leases_of_nothing_is_zero/1,
    send_carries_headers/1,
    grouped_head_takes_one_per_group/1,
    grouped_rr_round_robins_across_groups/1,
    ordering_within_a_group_is_strict/1,
    topic_binding_routes_by_pattern/1,
    topic_wildcards_match/1,
    unbind_topic_reports_whether_it_removed_anything/1,
    long_poll_returns_early_when_a_message_exists/1,
    long_poll_times_out_on_an_empty_queue/1,
    identifier_guard_rejects_injection/1
]).

all() ->
    [{group, basics}, {group, fifo}, {group, topics}, {group, polling}, {group, safety}].

groups() ->
    [
        {basics, [], [
            read_hides_behind_the_visibility_timeout,
            expired_lease_redelivers_with_higher_read_ct,
            archive_acks_a_batch,
            archive_of_an_unknown_id_is_empty,
            archive_of_nothing_touches_the_database,
            metrics_reports_queue_length,
            release_leases_frees_stranded_messages,
            release_leases_of_nothing_is_zero,
            send_carries_headers
        ]},
        {fifo, [], [
            grouped_head_takes_one_per_group,
            grouped_rr_round_robins_across_groups,
            ordering_within_a_group_is_strict
        ]},
        {topics, [], [
            topic_binding_routes_by_pattern,
            topic_wildcards_match,
            unbind_topic_reports_whether_it_removed_anything
        ]},
        {polling, [], [
            long_poll_returns_early_when_a_message_exists,
            long_poll_times_out_on_an_empty_queue
        ]},
        {safety, [], [identifier_guard_rejects_injection]}
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    ok = ergon_test_db:sandbox(),
    Queue = ergon_test_db:unique(~"pgmq_test"),
    ok = ergon_pgmq:create_queue(Queue),
    [{queue, Queue} | Config].

%% The queue goes with the rollback: pgmq.create is ordinary DDL plus an ordinary
%% insert, and PostgreSQL DDL is transactional.
end_per_testcase(_Case, _Config) ->
    ok = ergon_test_db:rollback().

%% ---------------
%% Basics
%% ---------------

read_hides_behind_the_visibility_timeout(Config) ->
    Q = ?config(queue, Config),
    {ok, Id} = ergon_pgmq:send(Q, #{~"ping" => 1}),

    ?assertMatch(
        {ok, [#{id := Id, read_ct := 1, message := #{~"ping" := 1}}]},
        ergon_pgmq:read(Q, 30, 10)
    ),
    %% Hidden until the lease expires, so a second consumer sees nothing.
    ?assertEqual({ok, []}, ergon_pgmq:read(Q, 30, 10)).

%% vt 0 expires the lease immediately, which is what a consumer dying
%% mid-processing looks like from the queue's side.
expired_lease_redelivers_with_higher_read_ct(Config) ->
    Q = ?config(queue, Config),
    {ok, Id} = ergon_pgmq:send(Q, #{~"ping" => 2}),
    ?assertMatch({ok, [#{id := Id, read_ct := 1}]}, ergon_pgmq:read(Q, 0, 10)),
    ?assertMatch({ok, [#{id := Id, read_ct := 2}]}, ergon_pgmq:read(Q, 30, 10)).

archive_acks_a_batch(Config) ->
    Q = ?config(queue, Config),
    Ids = [
        begin
            {ok, I} = ergon_pgmq:send(Q, #{~"n" => N}),
            I
        end
     || N <- [1, 2, 3]
    ],
    {ok, _} = ergon_pgmq:read(Q, 30, 10),

    {ok, Archived} = ergon_pgmq:archive(Q, Ids),
    ?assertEqual(lists:sort(Ids), lists:sort(Archived)),
    ?assertMatch({ok, #{queue_length := 0}}, ergon_pgmq:metrics(Q)).

archive_of_an_unknown_id_is_empty(Config) ->
    ?assertEqual({ok, []}, ergon_pgmq:archive(?config(queue, Config), [999999])).

%% An empty batch short-circuits rather than issuing a statement, which is what
%% makes a consumer cycle that processed nothing free.
archive_of_nothing_touches_the_database(Config) ->
    ?assertEqual({ok, []}, ergon_pgmq:archive(?config(queue, Config), [])).

%% queue_visible_length is computed against transaction-frozen now(), so a
%% message sent inside this fixture never counts as visible here. Assert on
%% queue_length, which is a plain count.
metrics_reports_queue_length(Config) ->
    Q = ?config(queue, Config),
    {ok, _} = ergon_pgmq:send(Q, #{~"n" => 10}),
    {ok, _} = ergon_pgmq:send(Q, #{~"n" => 11}),
    ?assertMatch({ok, #{queue_length := 2}}, ergon_pgmq:metrics(Q)).

release_leases_frees_stranded_messages(Config) ->
    Q = ?config(queue, Config),
    {ok, Id} = ergon_pgmq:send(Q, #{~"n" => 12}),

    %% A consumer takes an hour-long lease and dies without acking.
    ?assertMatch({ok, [#{id := Id, read_ct := 1}]}, ergon_pgmq:read(Q, 3600, 10)),
    ?assertEqual({ok, []}, ergon_pgmq:read(Q, 30, 10)),

    %% The reconciler frees it rather than waiting the hour out.
    ?assertEqual({ok, 1}, ergon_pgmq:release_leases(Q)),
    ?assertMatch({ok, [#{id := Id, read_ct := 2}]}, ergon_pgmq:read(Q, 30, 10)).

release_leases_of_nothing_is_zero(Config) ->
    ?assertEqual({ok, 0}, ergon_pgmq:release_leases(?config(queue, Config))).

send_carries_headers(Config) ->
    Q = ?config(queue, Config),
    {ok, _} = ergon_pgmq:send(Q, #{~"body" => 1}, #{~"trace" => ~"abc"}),
    {ok, [#{headers := Headers}]} = ergon_pgmq:read(Q, 30, 10),
    ?assertEqual(#{~"trace" => ~"abc"}, Headers).

%% ---------------
%% FIFO message groups
%% ---------------

%% Ordering is enforced by the visibility timeout, not by locking: the next
%% message in a group stays hidden until the one ahead of it is archived or its
%% lease expires.
grouped_head_takes_one_per_group(Config) ->
    Q = ?config(queue, Config),
    send_grouped(Q, ~"a", [1, 2]),
    send_grouped(Q, ~"b", [1, 2]),

    {ok, Messages} = ergon_pgmq:read(Q, 30, 10, grouped_head),
    ?assertEqual(2, length(Messages)),
    ?assertEqual([~"a", ~"b"], lists:sort([group_of(M) || M <- Messages])).

grouped_rr_round_robins_across_groups(Config) ->
    Q = ?config(queue, Config),
    %% A busy group and a quiet one. Round robin must not let the busy one
    %% starve the quiet one out of the batch.
    send_grouped(Q, ~"busy", [1, 2, 3, 4]),
    send_grouped(Q, ~"quiet", [1]),

    {ok, Messages} = ergon_pgmq:read(Q, 30, 2, grouped_rr),
    ?assertEqual([~"busy", ~"quiet"], lists:sort([group_of(M) || M <- Messages])).

ordering_within_a_group_is_strict(Config) ->
    Q = ?config(queue, Config),
    send_grouped(Q, ~"g", [1, 2, 3]),

    %% Only the head is available while it is leased.
    {ok, [First]} = ergon_pgmq:read(Q, 30, 10, grouped_head),
    ?assertEqual(1, body_of(First)),
    ?assertEqual({ok, []}, ergon_pgmq:read(Q, 30, 10, grouped_head)),

    %% Acking it releases the next one, and only the next one.
    {ok, _} = ergon_pgmq:archive(Q, [maps:get(id, First)]),
    {ok, [Second]} = ergon_pgmq:read(Q, 30, 10, grouped_head),
    ?assertEqual(2, body_of(Second)).

%% ---------------
%% Topics
%% ---------------

%% Pattern first, queue second, matching send_topic/2 where the routing key also
%% leads. Only the queue is validated as an identifier; the pattern is a bind
%% parameter and may contain the wildcard characters an identifier may not.
topic_binding_routes_by_pattern(Config) ->
    Q = ?config(queue, Config),
    ok = ergon_pgmq:bind_topic(~"orders.created", Q),

    ?assertEqual({ok, 1}, ergon_pgmq:send_topic(~"orders.created", #{~"id" => 1})),
    ?assertMatch({ok, [#{message := #{~"id" := 1}}]}, ergon_pgmq:read(Q, 30, 10)),

    %% A routing key nothing is bound to reaches nobody rather than erroring,
    %% which is what a topic exchange is supposed to do and also an easy way to
    %% lose messages to a typo. The count is the only signal.
    ?assertEqual({ok, 0}, ergon_pgmq:send_topic(~"orders.deleted", #{~"id" => 2})).

topic_wildcards_match(Config) ->
    Q = ?config(queue, Config),
    %% `*` matches exactly one segment, `#` matches zero or more.
    ok = ergon_pgmq:bind_topic(~"orders.*.shipped", Q),
    ok = ergon_pgmq:bind_topic(~"audit.#", Q),

    ?assertEqual({ok, 1}, ergon_pgmq:send_topic(~"orders.eu.shipped", #{~"n" => 1})),
    ?assertEqual({ok, 0}, ergon_pgmq:send_topic(~"orders.eu.west.shipped", #{~"n" => 2})),
    ?assertEqual({ok, 1}, ergon_pgmq:send_topic(~"audit.a.b.c", #{~"n" => 3})),

    {ok, Messages} = ergon_pgmq:read(Q, 30, 10),
    ?assertEqual([1, 3], lists:sort([maps:get(~"n", maps:get(message, M)) || M <- Messages])).

unbind_topic_reports_whether_it_removed_anything(Config) ->
    Q = ?config(queue, Config),
    ok = ergon_pgmq:bind_topic(~"events.one", Q),
    ?assertMatch({ok, [#{pattern := ~"events.one"}]}, ergon_pgmq:list_topic_bindings(Q)),

    ?assertEqual({ok, true}, ergon_pgmq:unbind_topic(~"events.one", Q)),
    ?assertEqual({ok, false}, ergon_pgmq:unbind_topic(~"events.one", Q)),
    ?assertEqual({ok, []}, ergon_pgmq:list_topic_bindings(Q)).

%% ---------------
%% Long polling
%% ---------------

%% The point of long polling is that a waiting consumer costs one blocked
%% connection rather than a stream of empty round trips. With a message already
%% present it must not wait at all.
long_poll_returns_early_when_a_message_exists(Config) ->
    Q = ?config(queue, Config),
    {ok, _} = ergon_pgmq:send(Q, #{~"n" => 1}),

    {Elapsed, Result} = timer:tc(fun() ->
        ergon_pgmq:read(Q, 30, 10, {long_poll, 5, 100})
    end),
    ?assertMatch({ok, [_]}, Result),
    ?assert(Elapsed < 2_000_000).

long_poll_times_out_on_an_empty_queue(Config) ->
    Q = ?config(queue, Config),
    {Elapsed, Result} = timer:tc(fun() ->
        ergon_pgmq:read(Q, 30, 10, {long_poll, 1, 100})
    end),
    ?assertEqual({ok, []}, Result),
    %% It really blocked server-side rather than returning immediately.
    ?assert(Elapsed >= 900_000).

%% ---------------
%% Safety
%% ---------------

%% A queue name reaches SQL as an identifier, since pgmq derives `pgmq.q_<name>`
%% from it and a table cannot be addressed by bind parameter.
identifier_guard_rejects_injection(_Config) ->
    ?assertError(invalid_identifier, ergon_pgmq:create_queue(~"bad; DROP TABLE x --")),
    ?assertError(invalid_identifier, ergon_pgmq:enable_notify(~"1starts_with_digit")),
    ?assertError(invalid_identifier, ergon_pgmq:create_queue(~"MixedCase")).

%% ---------------
%% Helpers
%% ---------------

send_grouped(Queue, Group, Bodies) ->
    [
        begin
            {ok, Id} = ergon_pgmq:send(Queue, #{~"n" => N}, ergon_pgmq:group_header(Group)),
            Id
        end
     || N <- Bodies
    ].

group_of(#{headers := #{~"x-pgmq-group" := Group}}) -> Group.

body_of(#{message := #{~"n" := N}}) -> N.
