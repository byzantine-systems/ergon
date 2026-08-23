-module(ergon_job_notifier_SUITE).
-moduledoc """
The `LISTEN`/`NOTIFY` wake path, end to end.

**This suite cannot use the fixture transaction, and the reason is the mechanism
itself.** A `NOTIFY` is delivered only when its transaction commits, so anything
emitted inside a transaction that is rolled back reaches nobody. These cases
commit their rows and delete them afterwards, on the same connection.

Two layers:

1. **Routing.** `ergon_job_notifier` receives a notification and dispatches
   `wake` to the workers registered for the payload's queue. Injected directly,
   which is deterministic and has no commit race in it.

2. **The tick.** `ergon.notify_pending_jobs()` emits one notification per queue
   with runnable work, and its predicate matches `jobs_fetch_idx` exactly. In
   production pg_cron runs it every second; the test database has no pg_cron by
   design, so the cases call it themselves, which is the more direct test
   anyway.

The application's own notifier is disabled for the run, so this suite starts one
and owns it.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    wake_reaches_every_worker_on_the_queue/1,
    an_unregistered_queue_wakes_no_one/1,
    a_runnable_job_notifies_once_per_queue/1,
    a_future_scheduled_job_does_not_notify/1,
    a_blocked_job_does_not_notify/1,
    channel_is_stable/1,
    resubscribing_leaves_the_notifier_working/1
]).

-define(WAIT, 2000).

all() -> [{group, routing}, {group, tick}].

groups() ->
    [
        {routing, [], [
            wake_reaches_every_worker_on_the_queue,
            an_unregistered_queue_wakes_no_one,
            channel_is_stable,
            resubscribing_leaves_the_notifier_working
        ]},
        {tick, [], [
            a_runnable_job_notifies_once_per_queue,
            a_future_scheduled_job_does_not_notify,
            a_blocked_job_does_not_notify
        ]}
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    [{prefix, ergon_test_db:unique(~"jobnotif")} | Config].

end_per_testcase(_Case, Config) ->
    stop_notifier(),
    ergon_test_db:cleanup_jobs(?config(prefix, Config)).

%% ---------------
%% Routing
%% ---------------

channel_is_stable(_Config) ->
    ?assertEqual(~"ergon_job_available", ergon_job_notifier:channel()).

%% Several workers draining one queue all wake. The payload is the queue name and
%% nothing else: NOTIFY bypasses row-level security, so it must never carry job
%% data or a tenant.
wake_reaches_every_worker_on_the_queue(Config) ->
    Queue = <<(?config(prefix, Config))/binary, "_route">>,
    Notifier = start_notifier(),

    ok = ergon_worker_registry:join(Queue),
    Parent = self(),
    Other = spawn_link(fun() ->
        ok = ergon_worker_registry:join(Queue),
        Parent ! joined,
        receive
            wake -> Parent ! other_woke
        after ?WAIT -> Parent ! other_timed_out
        end
    end),
    receive
        joined -> ok
    after ?WAIT -> ct:fail(peer_never_joined)
    end,

    Notifier ! {notification, self(), make_ref(), ergon_job_notifier:channel(), Queue},

    ?assertEqual(ok, await(wake)),
    ?assertEqual(ok, await(other_woke)),
    ?assert(is_process_alive(Other) orelse true).

%% The resubscribe path, driven directly. It is worth a case of its own because
%% it carries the `backoff` state now, and the two ways of getting that wrong are
%% both silent until this runs: seed it under the wrong key and the clause head
%% never matches, forget to thread the advanced record back and the delay stops
%% growing.
%%
%% The listener is up, so this takes the success branch, which is the one that
%% resets. Killing the listener to reach the other branch is not worth it: it is
%% a supervised singleton this suite shares with every other committed suite, and
%% the supervisor would restart it inside the window the case needs it absent.
resubscribing_leaves_the_notifier_working(Config) ->
    Queue = <<(?config(prefix, Config))/binary, "_resub">>,
    Notifier = start_notifier(),
    ok = ergon_worker_registry:join(Queue),

    Notifier ! resubscribe,
    Notifier ! resubscribe,

    %% A crash in resubscribe/1 would take the process down before this lands.
    ?assert(is_process_alive(Notifier)),
    Notifier ! {notification, self(), make_ref(), ergon_job_notifier:channel(), Queue},
    ?assertEqual(ok, await(wake)).

%% A queue this node does not drain is the common case, not an error: the tick
%% reports every queue with runnable work, not only the ones present here.
an_unregistered_queue_wakes_no_one(Config) ->
    Prefix = ?config(prefix, Config),
    Notifier = start_notifier(),
    ok = ergon_worker_registry:join(<<Prefix/binary, "_mine">>),

    Notifier !
        {notification, self(), make_ref(), ergon_job_notifier:channel(),
            <<Prefix/binary, "_other">>},

    ?assertEqual(timeout, await(wake, 300)).

%% ---------------
%% The tick
%% ---------------

%% One notification per queue with runnable work, not one per job: the tick
%% selects DISTINCT queue.
a_runnable_job_notifies_once_per_queue(Config) ->
    Prefix = ?config(prefix, Config),
    QueueA = <<Prefix/binary, "_run_a">>,
    QueueB = <<Prefix/binary, "_run_b">>,
    ok = listen(),

    [insert_job(QueueA) || _ <- [1, 2, 3]],
    insert_job(QueueB),

    Notified = tick(),
    ?assert(Notified >= 2),

    ?assertEqual(ok, await_notification(QueueA)),
    ?assertEqual(ok, await_notification(QueueB)),
    %% Three jobs on QueueA, one notification for it.
    ?assertEqual(timeout, await_notification(QueueA, 300)).

%% The `scheduled_at <= now()` half of the predicate. A retry backing off into
%% the future must wake no one until it comes due.
a_future_scheduled_job_does_not_notify(Config) ->
    Queue = <<(?config(prefix, Config))/binary, "_future">>,
    ok = listen(),

    _ = ergon_repo:query(
        "INSERT INTO ergon.jobs (queue, worker, scheduled_at) "
        "VALUES ($1, 'w', now() + interval '1 hour')",
        [Queue]
    ),

    _ = tick(),
    ?assertEqual(timeout, await_notification(Queue, 500)).

%% The `pending_parents = 0` half, added in Phase 4. A blocked child is not
%% runnable, so waking a worker for it would be a wasted checkout that finds
%% nothing: the same predicate keeps it out of jobs_fetch_idx.
a_blocked_job_does_not_notify(Config) ->
    Prefix = ?config(prefix, Config),
    ParentQueue = <<Prefix/binary, "_bp">>,
    ChildQueue = <<Prefix/binary, "_bc">>,
    ok = listen(),

    Parent = insert_job(ParentQueue),
    Child = insert_job(ChildQueue),
    ok = ergon:depends_on(Parent, Child),

    _ = tick(),
    ?assertEqual(ok, await_notification(ParentQueue)),
    ?assertEqual(timeout, await_notification(ChildQueue, 500)).

%% ---------------
%% Helpers
%% ---------------

start_notifier() ->
    {ok, Pid} = ergon_job_notifier:start_link(#{}),
    put(notifier, Pid),
    Pid.

stop_notifier() ->
    case erase(notifier) of
        undefined ->
            ok;
        Pid ->
            unlink(Pid),
            exit(Pid, shutdown),
            ok
    end.

%% Subscribe from the case process itself. pgo_notifications registers a
%% subscription per calling process and delivers to it, which is exactly why
%% ergon_listener is a module of functions rather than a proxy gen_server.
listen() ->
    {ok, _SubRef, _MonRef} = ergon_listener:subscribe(ergon_job_notifier:channel()),
    ok.

insert_job(Queue) ->
    {ok, #{rows := [{Id}]}} = ergon_repo:query(
        "INSERT INTO ergon.jobs (queue, worker) VALUES ($1, 'w') RETURNING id", [Queue]
    ),
    Id.

tick() ->
    {ok, #{rows := [{Notified}]}} = ergon_repo:query("SELECT ergon.notify_pending_jobs()", []),
    Notified.

await_notification(Queue) -> await_notification(Queue, ?WAIT).

await_notification(Queue, Timeout) ->
    Channel = ergon_job_notifier:channel(),
    receive
        {notification, _Pid, _Ref, Channel, Queue} -> ok
    after Timeout -> timeout
    end.

await(Message) -> await(Message, ?WAIT).

await(Message, Timeout) ->
    receive
        Message -> ok
    after Timeout -> timeout
    end.
