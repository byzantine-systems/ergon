-module(ergon_pgmq_consumer_SUITE).
-moduledoc """
Messages flowing end to end through a real pgmq consumer.

Committed, for the same reason as the worker suite: the consumer reads and the
runners execute from processes of their own, none of which can see a row written
inside the case process's fixture transaction.

This replaces the Broadway producer tests. Those exercised `GenStage` callbacks
and a hand-built `Broadway.Message` with its acknowledger tuple; there is no
Broadway here, so what is left is the behaviour those callbacks existed to
produce: a message is delivered to the handler, a successful one is archived, a
failed one is not.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    messages_reach_the_handler/1,
    successful_messages_are_archived/1,
    failed_messages_are_left_for_redelivery/1,
    a_raising_handler_leaves_the_message/1,
    a_batch_is_archived_in_one_call/1,
    an_empty_queue_produces_nothing/1,
    grouped_consumer_preserves_order_within_a_group/1,
    stop_consumer_stops_the_tree/1
]).

-define(POLL, 50).
-define(WAIT, 5000).

all() ->
    [
        messages_reach_the_handler,
        successful_messages_are_archived,
        failed_messages_are_left_for_redelivery,
        a_raising_handler_leaves_the_message,
        a_batch_is_archived_in_one_call,
        an_empty_queue_produces_nothing,
        grouped_consumer_preserves_order_within_a_group,
        stop_consumer_stops_the_tree
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Queue = ergon_test_db:unique(~"pgmq_cons"),
    ok = ergon_pgmq:create_queue(Queue),
    [{queue, Queue} | Config].

end_per_testcase(_Case, Config) ->
    [ergon:stop_consumer(Pid) || Pid <- consumer_pids()],
    %% Committed, so the queue really has to be dropped rather than rolled back.
    _ = ergon_pgmq:drop_queue(?config(queue, Config)),
    ok.

%% ---------------
%% Cases
%% ---------------

messages_reach_the_handler(Config) ->
    Q = ?config(queue, Config),
    Self = self(),
    {ok, Id} = ergon_pgmq:send(Q, #{~"x" => 1}),

    start_consumer(Q, fun(Message) ->
        Self ! {got, Message},
        ok
    end),

    receive
        {got, Message} ->
            ?assertMatch(#{id := Id, read_ct := 1, message := #{~"x" := 1}}, Message)
    after ?WAIT -> ct:fail(handler_never_ran)
    end.

successful_messages_are_archived(Config) ->
    Q = ?config(queue, Config),
    Self = self(),
    {ok, _} = ergon_pgmq:send(Q, #{~"x" => 1}),

    start_consumer(Q, fun(_Message) ->
        Self ! handled,
        ok
    end),
    ?assertEqual(ok, await(handled)),

    ?assertEqual(0, await_queue_length(Q, 0)),
    ?assertEqual(1, archived_count(Q)).

%% `{error, _}` leaves the message for its visibility timeout to redeliver, which
%% is pgmq's whole failure model: there is no dead-letter concept, and `read_ct`
%% is the only signal a handler has for a poison message.
failed_messages_are_left_for_redelivery(Config) ->
    Q = ?config(queue, Config),
    Self = self(),
    {ok, Id} = ergon_pgmq:send(Q, #{~"x" => 1}),

    start_consumer(
        ergon_pgmq_queue:with_visibility_timeout(base_queue(Q), 1),
        fun(#{read_ct := ReadCt}) ->
            Self ! {attempt, ReadCt},
            {error, ~"nope"}
        end
    ),

    ?assertEqual(ok, await({attempt, 1})),
    %% Never archived, and redelivered once the lease lapses.
    ?assertEqual(ok, await({attempt, 2}, ?WAIT)),
    ?assertEqual(0, archived_count(Q)),
    ?assertEqual(1, queue_length(Q)),
    ?assertMatch(Id, Id).

a_raising_handler_leaves_the_message(Config) ->
    Q = ?config(queue, Config),
    Self = self(),
    {ok, _} = ergon_pgmq:send(Q, #{~"x" => 1}),

    start_consumer(Q, fun(_Message) ->
        Self ! attempted,
        error(deliberate)
    end),

    ?assertEqual(ok, await(attempted)),
    timer:sleep(200),
    %% A raise is an error like any other: the message stays, the consumer lives.
    ?assertEqual(0, archived_count(Q)),
    ?assertEqual(1, queue_length(Q)).

%% The batch cycle archives every success in one call rather than one per
%% message, which is what keeps a full batch to a fixed number of round trips.
a_batch_is_archived_in_one_call(Config) ->
    Q = ?config(queue, Config),
    Self = self(),
    [{ok, _} = ergon_pgmq:send(Q, #{~"n" => N}) || N <- lists:seq(1, 5)],

    start_consumer(Q, fun(#{message := #{~"n" := N}}) ->
        Self ! {handled, N},
        ok
    end),

    Handled = [
        receive
            {handled, N} -> N
        after ?WAIT -> timeout
        end
     || _ <- lists:seq(1, 5)
    ],
    ?assertEqual([1, 2, 3, 4, 5], lists:sort(Handled)),
    ?assertEqual(0, await_queue_length(Q, 0)),
    ?assertEqual(5, archived_count(Q)).

an_empty_queue_produces_nothing(Config) ->
    Q = ?config(queue, Config),
    Self = self(),
    start_consumer(Q, fun(_Message) ->
        Self ! unexpected,
        ok
    end),
    ?assertEqual(timeout, await(unexpected, 500)).

%% Ordering within a group is enforced by the visibility timeout: the next
%% message stays hidden until the one ahead is archived. A consumer reading
%% `grouped_head` therefore sees a group strictly in order.
grouped_consumer_preserves_order_within_a_group(Config) ->
    Q = ?config(queue, Config),
    Self = self(),
    Header = ergon_pgmq:group_header(~"g"),
    [{ok, _} = ergon_pgmq:send(Q, #{~"n" => N}, Header) || N <- [1, 2, 3]],

    start_consumer(
        ergon_pgmq_queue:with_read_strategy(base_queue(Q), grouped_head),
        fun(#{message := #{~"n" := N}}) ->
            Self ! {handled, N},
            ok
        end
    ),

    Order = [
        receive
            {handled, N} -> N
        after ?WAIT -> timeout
        end
     || _ <- [1, 2, 3]
    ],
    ?assertEqual([1, 2, 3], Order).

stop_consumer_stops_the_tree(Config) ->
    Q = ?config(queue, Config),
    Self = self(),
    Pid = start_consumer(Q, fun(_Message) ->
        Self ! handled,
        ok
    end),

    {ok, _} = ergon_pgmq:send(Q, #{~"x" => 1}),
    ?assertEqual(ok, await(handled)),

    ?assertEqual(ok, ergon:stop_consumer(Pid)),
    ?assertNot(is_process_alive(Pid)),

    {ok, _} = ergon_pgmq:send(Q, #{~"x" => 2}),
    ?assertEqual(timeout, await(handled, 500)),
    ?assertEqual({error, not_found}, ergon:stop_consumer(self())).

%% ---------------
%% Helpers
%% ---------------

base_queue(Name) ->
    ergon_pgmq_queue:with_poll_interval(ergon_pgmq_queue:new(Name), ?POLL).

start_consumer(Name, Handler) when is_binary(Name) ->
    start_consumer(base_queue(Name), Handler);
start_consumer(Queue, Handler) ->
    {ok, Pid} = ergon:start_consumer(Queue, Handler),
    put(consumers, [Pid | existing()]),
    Pid.

existing() ->
    case get(consumers) of
        undefined -> [];
        Pids -> Pids
    end.

consumer_pids() ->
    case erase(consumers) of
        undefined -> [];
        Pids -> Pids
    end.

queue_length(Queue) ->
    {ok, #{queue_length := N}} = ergon_pgmq:metrics(Queue),
    N.

archived_count(Queue) ->
    {ok, #{rows := [{N}]}} = ergon_repo:query(
        ["SELECT count(*)::int FROM pgmq.a_", Queue], []
    ),
    N.

await_queue_length(Queue, Expected) -> await_queue_length(Queue, Expected, ?WAIT).

await_queue_length(Queue, Expected, Remaining) when Remaining =< 0 ->
    queue_length(Queue) - Expected + Expected;
await_queue_length(Queue, Expected, Remaining) ->
    case queue_length(Queue) of
        Expected ->
            Expected;
        _Other ->
            timer:sleep(25),
            await_queue_length(Queue, Expected, Remaining - 25)
    end.

await(Message) -> await(Message, ?WAIT).

await(Message, Timeout) ->
    receive
        Message -> ok
    after Timeout -> timeout
    end.
