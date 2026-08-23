-module(ergon_sup).
-moduledoc """
Ergon's root supervisor.

Child order is the dependency order, and every step of it matters:

```text
ergon_sql                  every consumer runs named statements through it
ergon_listener             the node's shared LISTEN connection
ergon_worker_registry      the pg scope; workers join it in their own init/1
ergon_job_notifier         optional; dispatches wakes through the registry
ergon_worker_sup           dynamic; queue workers appear under it at runtime
ergon_pgmq_consumer_sup    dynamic; pgmq consumers likewise
```

`one_for_one`, so a notifier crash does not disturb running workers. The
notifier is the only optional child: without it workers still drain on their
fallback poll, correctly but with more latency.

`ergon_listener` precedes both the notifier and the consumer supervisor, because
both subscribe to it from their own `init/1`. It is started unconditionally even
when the job notifier is disabled, since a pgmq consumer may still want it.

The connection pool is **not** a child. `ergon_app:start/2` starts it before this
tree and `pgo_sup` supervises it; see `ergon_repo` for why that is not turned
into a child spec.
""".

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    Children =
        [
            #{
                id => ergon_sql,
                start => {ergon_sql, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [ergon_sql]
            },
            ergon_listener:child_spec(),
            ergon_worker_registry:child_spec()
        ] ++ job_notifier() ++
            [
                #{
                    id => ergon_worker_sup,
                    start => {ergon_worker_sup, start_link, []},
                    restart => permanent,
                    shutdown => infinity,
                    type => supervisor,
                    modules => [ergon_worker_sup]
                },
                #{
                    id => ergon_pgmq_consumer_sup,
                    start => {ergon_pgmq_consumer_sup, start_link, []},
                    restart => permanent,
                    shutdown => infinity,
                    type => supervisor,
                    modules => [ergon_pgmq_consumer_sup]
                }
            ],
    {ok, {SupFlags, Children}}.

%% The reactive LISTEN path is opt-out. Disabled, workers drain on their periodic
%% poll: still fully correct, only slower. Tests disable it because a notification
%% only fires on commit, so a listener would never see anything from inside a
%% rolled-back fixture transaction.
job_notifier() ->
    case ergon_job_notifier:enabled() of
        false ->
            [];
        true ->
            [
                #{
                    id => ergon_job_notifier,
                    start => {ergon_job_notifier, start_link, []},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [ergon_job_notifier]
                }
            ]
    end.
