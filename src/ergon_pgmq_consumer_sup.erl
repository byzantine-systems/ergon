-module(ergon_pgmq_consumer_sup).
-moduledoc """
The dynamic supervisor pgmq consumers run under.

The pgmq sibling of `ergon_worker_sup`, and for the same reason: which queues to
consume, with what concurrency and which handler, is the host's decision rather
than the library's, so consumers are added at runtime instead of listed here.

Each call adds one `ergon_pgmq_sup` (an executor pool and its consumer, plus a
dedicated read pool when long polling) supervised independently, so one queue
collapsing does not disturb the others.

`transient` restart: a consumer tree that exits abnormally comes back, one shut
down deliberately stays down.
""".

-behaviour(supervisor).

-export([start_link/0, start_consumer/2, stop_consumer/1]).
-export([init/1]).

-include_lib("ergon/include/ergon.hrl").

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 5, period => 10},
    ChildSpec = #{
        id => ergon_pgmq_sup,
        start => {ergon_pgmq_sup, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => supervisor,
        modules => [ergon_pgmq_sup]
    },
    {ok, {SupFlags, [ChildSpec]}}.

-doc """
Start a supervised consumer draining `Queue`, running `Handler` on each message.

Returns the pid of the consumer's supervisor, which is what `stop_consumer/1`
takes.
""".
-spec start_consumer(pgmq_queue(), pgmq_handler()) -> {ok, pid()} | {error, term()}.
start_consumer(#{name := Name} = Queue, Handler) when
    is_binary(Name), is_function(Handler, 1)
->
    supervisor:start_child(?MODULE, [Queue, Handler]).

-doc "Stop a consumer started by `start_consumer/2`, along with its pools.".
-spec stop_consumer(pid()) -> ok | {error, not_found}.
stop_consumer(Pid) when is_pid(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).
