-module(ergon_queue_sup).
-moduledoc """
Everything one call to `ergon:start_worker/2` creates: an executor pool and the
poller that feeds it.

```text
ergon_queue_sup (rest_for_one)
  |- wpool          concurrency x ergon_job_runner
  \- ergon_worker   the poller
```

`rest_for_one` and this order together encode the dependency. The poller casts
into the pool, so the pool must exist first, and if it dies the poller has to
come back with it. Otherwise the poller would go on casting into a name that no
longer resolves, checking out jobs that nothing would run. The reverse is not
true: a poller crash leaves the pool's in-flight jobs to finish normally.

## The in-flight counter

Created here rather than in either child, because both need the same reference
and it has to outlive a restart of either. `ergon_worker` reads it to size each
checkout; `ergon_job_runner` decrements it when a job finishes.

One consequence worth knowing: if the *pool* restarts, jobs its runners were
executing die with it and their slots are never returned, so the counter drifts
upward and the queue's effective concurrency drops. `rest_for_one` handles this
by restarting the poller too, but the counter itself is only reset when this
supervisor restarts. That is the intended blast radius: a pool that crashes
repeatedly should take its queue down rather than silently throttle it.
""".

-behaviour(supervisor).

-export([start_link/2, pool_name/1]).
-export([init/1]).

-include_lib("ergon/include/ergon.hrl").

-define(IN_FLIGHT_SIZE, 1).

-spec start_link(queue(), handler()) -> {ok, pid()} | {error, term()}.
start_link(Queue, Handler) ->
    supervisor:start_link(?MODULE, {Queue, Handler}).

-doc """
The registered name of a queue's executor pool.

Derived from the queue name, so it is stable across restarts and predictable
from the outside for `wpool:stats/1`. `binary_to_atom/2` is safe here because
queue names come from the host's own configuration, not from the database.
""".
-spec pool_name(binary()) -> atom().
pool_name(QueueName) when is_binary(QueueName) ->
    binary_to_atom(<<"ergon_pool_", QueueName/binary>>, utf8).

-spec init({queue(), handler()}) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Queue, Handler}) ->
    #{
        name := Name,
        concurrency := Concurrency,
        handler_timeout := HandlerTimeout
    } = Queue,

    InFlight = counters:new(?IN_FLIGHT_SIZE, [write_concurrency]),
    Pool = pool_name(Name),

    RunnerArgs = #{
        queue => Name,
        handler => Handler,
        handler_timeout => HandlerTimeout,
        in_flight => InFlight
    },

    SupFlags = #{strategy => rest_for_one, intensity => 5, period => 10},

    Children = [
        %% overrun_warning logs a handler that runs long. max_overrun_warnings
        %% stays at its `infinity` default on purpose: wpool's own enforcement
        %% kills the executor, which abandons the job with no outcome written.
        %% ergon_job_runner's handler_timeout kills the handler instead and still
        %% records the failure, so the attempt is consumed and the job retried.
        wpool:child_spec(Pool, [
            {workers, Concurrency},
            {worker, {ergon_job_runner, RunnerArgs}},
            {overrun_warning, overrun_warning(HandlerTimeout)},
            {queue_type, fifo}
        ]),
        #{
            id => ergon_worker,
            start =>
                {ergon_worker, start_link, [
                    #{queue => Queue, pool => Pool, in_flight => InFlight}
                ]},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [ergon_worker]
        }
    ],
    {ok, {SupFlags, Children}}.

%% Warn before the deadline rather than at it, so a handler that is merely slow
%% shows up in the log before it is killed. With no deadline configured there is
%% nothing to anticipate, so warn at a fixed minute.
overrun_warning(infinity) -> timer:minutes(1);
overrun_warning(Timeout) -> max(1, Timeout div 2).
