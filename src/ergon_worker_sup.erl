-module(ergon_worker_sup).
-moduledoc """
The dynamic supervisor queue workers run under.

Workers are added at runtime with `start_worker/2`, exposed through
`ergon:start_worker/2`, rather than being listed statically: which queues to
drain, with what concurrency, and with which handler is the host application's
decision, not the library's.

Each call adds one `ergon_queue_sup` (a pool and its poller) supervised
independently, so one queue collapsing does not disturb the others.

`transient` restart: a queue tree that exits abnormally comes back, but one shut
down deliberately stays down.
""".

-behaviour(supervisor).

-export([start_link/0, start_worker/2, stop_worker/1]).
-export([init/1]).

-include_lib("ergon/include/ergon.hrl").

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 5, period => 10},
    ChildSpec = #{
        id => ergon_queue_sup,
        start => {ergon_queue_sup, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => supervisor,
        modules => [ergon_queue_sup]
    },
    {ok, {SupFlags, [ChildSpec]}}.

-doc """
Start a supervised worker draining `Queue`, running `Handler` on each job.

Returns the pid of the queue's supervisor, which is what `stop_worker/1` takes.
Calling this twice for the same queue is allowed and useful: the two workers
race to drain and `FOR UPDATE SKIP LOCKED` keeps them from colliding. Their
executor pools would collide on a registered name, though, so prefer raising
`concurrency` for in-process parallelism and reserve a second worker for a
genuinely separate configuration.
""".
-spec start_worker(queue(), handler()) -> {ok, pid()} | {error, term()}.
start_worker(#{name := Name} = Queue, Handler) when is_binary(Name), is_function(Handler, 1) ->
    supervisor:start_child(?MODULE, [Queue, Handler]).

-doc "Stop a worker started by `start_worker/2`, along with its executor pool.".
-spec stop_worker(pid()) -> ok | {error, not_found}.
stop_worker(Pid) when is_pid(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).
