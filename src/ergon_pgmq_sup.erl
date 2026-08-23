-module(ergon_pgmq_sup).
-moduledoc """
Everything one call to `ergon:start_consumer/2` creates.

```text
ergon_pgmq_sup (rest_for_one)
  |- pgo pool       only when long polling; a single dedicated connection
  |- wpool          concurrency x ergon_pgmq_runner
  \- consumer       the batch cycle
```

`rest_for_one` in dependency order, as in `ergon_queue_sup`: the consumer casts
into the pool, so the pool must exist first and the consumer must come back with
it. A consumer crash leaves the runners to finish what they hold.

## The long-poll connection

A long-polling read blocks server-side for up to `max_poll_seconds`, holding its
connection for the whole call. Issued against the shared pool that would take a
connection out of circulation for every other query on the node, and with a few
such consumers it would empty the pool entirely.

So a long-polling consumer gets a `pgo` pool of its own, sized one, and its reads
are pinned to it through the `query_options()` that `ergon_sql:query/3` threads
down to the driver. Consumers that do not long poll share the main pool as
everything else does: their reads return immediately.

`pgo` pools live under `pgo_sup` rather than here, for the same reason
`ergon_repo`'s does, so this supervisor starts it in `init/1` and does not list
it as a child.
""".

-behaviour(supervisor).

-export([start_link/2, pool_name/1, read_pool_name/1]).
-export([init/1]).

-include_lib("ergon/include/ergon.hrl").

-spec start_link(pgmq_queue(), pgmq_handler()) -> {ok, pid()} | {error, term()}.
start_link(Queue, Handler) ->
    supervisor:start_link(?MODULE, {Queue, Handler}).

-doc "The registered name of a consumer's executor pool.".
-spec pool_name(binary()) -> atom().
pool_name(QueueName) when is_binary(QueueName) ->
    binary_to_atom(<<"ergon_pgmq_pool_", QueueName/binary>>, utf8).

-doc "The registered name of a long-polling consumer's dedicated read pool.".
-spec read_pool_name(binary()) -> atom().
read_pool_name(QueueName) when is_binary(QueueName) ->
    binary_to_atom(<<"ergon_pgmq_read_", QueueName/binary>>, utf8).

-spec init({pgmq_queue(), pgmq_handler()}) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Queue, Handler}) ->
    #{name := Name, concurrency := Concurrency} = Queue,
    HandlerTimeout = ergon_pgmq_queue:effective_handler_timeout(Queue),
    Pool = pool_name(Name),
    ReadOpts = read_opts(Queue),

    RunnerArgs = #{
        queue => Name,
        handler => Handler,
        handler_timeout => HandlerTimeout
    },

    SupFlags = #{strategy => rest_for_one, intensity => 5, period => 10},

    Children = [
        %% overrun_warning logs a handler running long; max_overrun_warnings
        %% stays at its infinity default deliberately. wpool's own enforcement
        %% kills the executor, which would leave the consumer waiting out its
        %% batch deadline for a reply that is never coming. The runner's own
        %% handler_timeout kills the handler instead and still answers.
        wpool:child_spec(Pool, [
            {workers, Concurrency},
            {worker, {ergon_pgmq_runner, RunnerArgs}},
            {overrun_warning, overrun_warning(HandlerTimeout)},
            {queue_type, fifo}
        ]),
        #{
            id => ergon_pgmq_consumer,
            start =>
                {ergon_pgmq_consumer, start_link, [
                    #{queue => Queue, pool => Pool, read_opts => ReadOpts}
                ]},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [ergon_pgmq_consumer]
        }
    ],
    {ok, {SupFlags, Children}}.

%% A long-polling consumer reads on a pool of its own; everyone else uses the
%% default, which is what an empty options map selects.
read_opts(#{name := Name} = Queue) ->
    case ergon_pgmq_queue:long_poll(Queue) of
        false ->
            #{};
        true ->
            ReadPool = read_pool_name(Name),
            {ok, _} = ergon_repo:start_pool(ReadPool, #{pool_size => 1}),
            #{pool => ReadPool}
    end.

overrun_warning(infinity) -> timer:minutes(1);
overrun_warning(Timeout) -> max(1, Timeout div 2).
