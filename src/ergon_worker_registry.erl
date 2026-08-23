-module(ergon_worker_registry).
-moduledoc """
Routes `NOTIFY` wake-ups from `ergon_job_notifier` to the workers that care.

A thin wrapper over OTP's `pg`, using the queue name as the group. Every
`ergon_worker` joins its queue's group on init, and several workers draining the
same queue coexist under one group, which is exactly `pg`'s semantics. When a job
lands, `wake/1` fans a `wake` message out to them; they then race to drain, and
`FOR UPDATE SKIP LOCKED` in `checkout` sorts out the contention exactly as it
does on a periodic poll. A duplicate or spurious wake is therefore always
harmless.

`pg` lives in `kernel`, so this costs no dependency.

## The wake is deliberately node-local

`wake/1` uses `pg:get_local_members/2`, not `get_members/2`, even though `pg`
synchronises group membership across the whole cluster and the cluster-wide call
would "work".

Every node runs its own notifier and receives the same `NOTIFY`, so waking
remote workers means each worker is woken once per node in the cluster. That is
harmless but pointless, and it quietly turns a local message send into
cluster-wide chatter proportional to the square of the node count. Keeping it
local also preserves the semantics a single-node deployment already has, so
adding a second node changes throughput and nothing else.

Cluster-wide membership is still useful for observability, which is why `pg` is
the right primitive rather than a private ETS table: `pg:get_members/2` answers
"who is draining this queue anywhere?" without any extra machinery.

## This is the fast path only

If the notifier is disabled, or a wake is lost, workers still drain on their
periodic fallback poll. The registry never being reached costs latency, never
correctness.
""".

-export([
    child_spec/0,
    scope/0,
    join/1,
    members/1,
    local_members/1,
    wake/1
]).

-define(SCOPE, ?MODULE).

-doc """
Child spec for the `pg` scope process.

Started ahead of both `ergon_job_notifier` and `ergon_worker_sup` in
`ergon_sup`, so a worker can join the moment it boots.
""".
-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?SCOPE,
        start => {pg, start_link, [?SCOPE]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [pg]
    }.

-doc "The `pg` scope Ergon's worker groups live in.".
-spec scope() -> atom().
scope() -> ?SCOPE.

-doc """
Join the calling process to `QueueName`'s group.

Called from `ergon_worker:init/1`. `pg` monitors members and removes them on
exit, so a crashed or stopped worker needs no explicit deregistration.
""".
-spec join(binary()) -> ok.
join(QueueName) when is_binary(QueueName) ->
    pg:join(?SCOPE, QueueName, self()).

-doc "Every worker draining `QueueName`, on any node in the cluster.".
-spec members(binary()) -> [pid()].
members(QueueName) when is_binary(QueueName) ->
    pg:get_members(?SCOPE, QueueName).

-doc "Every worker draining `QueueName` on this node.".
-spec local_members(binary()) -> [pid()].
local_members(QueueName) when is_binary(QueueName) ->
    pg:get_local_members(?SCOPE, QueueName).

-doc """
Send `wake` to every worker draining `QueueName` on this node.

A no-op when none are registered, which is the normal case for a queue this node
does not drain: the notifier sees every queue with runnable work, not only the
ones present here.
""".
-spec wake(binary()) -> ok.
wake(QueueName) when is_binary(QueueName) ->
    _ = [Pid ! wake || Pid <- local_members(QueueName)],
    ok.
