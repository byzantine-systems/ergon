-module(ergon_pgmq_runner).
-moduledoc """
Executes one pgmq message and reports the result back to its consumer.

`worker_pool` starts `concurrency` of these per queue. Unlike `ergon_job_runner`,
this one does not persist anything: acknowledgement is archiving, archiving is a
batch operation, and the consumer is the only process that knows when a batch is
complete. So the runner's whole job is to run the handler and answer.

The handler runs in a child process under a deadline, for the same reasons as on
the job side: a handler is host code that may return garbage, raise, exit, or
hang, and all four have to cost one message rather than an executor.
`spawn_request/2` with its own reply and monitor tags keeps those two replies
distinct from the wpool protocol traffic sharing this mailbox.

A timeout here means the visibility timeout has effectively run out (that is the
default deadline), so the message is already deliverable again and possibly
already in another consumer's hands. Reporting it as failed is not merely
convenient, it is the only correct answer: archiving afterwards would remove a
message someone else is still working on.
""".

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2]).

-include_lib("ergon/include/ergon.hrl").

-type args() :: #{
    queue := binary(),
    handler := pgmq_handler(),
    handler_timeout := timeout()
}.
-export_type([args/0]).

-spec start_link(args()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec init(args()) -> {ok, args()}.
init(#{queue := Queue} = Args) ->
    proc_lib:set_label({?MODULE, Queue}),
    {ok, Args}.

%% Reply carries the message id as well as the batch reference, so the consumer
%% can tell which of the batch it is hearing about without tracking a reference
%% per message.
handle_cast({run, Consumer, BatchRef, #{id := MsgId} = Message}, State) ->
    Consumer ! {pgmq_done, BatchRef, MsgId, execute(State, Message)},
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

%% ---------------
%% Execution
%% ---------------

-spec execute(args(), pgmq_message()) -> ok | {error, binary()}.
execute(#{handler := Handler, handler_timeout := Timeout}, Message) ->
    ReqId = spawn_request(
        fun() -> exit({ergon_result, safe_run(Handler, Message)}) end,
        [
            {reply, error_only},
            {reply_tag, ergon_spawn},
            {monitor, [{tag, ergon_down}]}
        ]
    ),
    receive
        {ergon_down, ReqId, process, _Pid, {ergon_result, Result}} ->
            Result;
        %% Exited without producing a result: killed from outside, or brought
        %% down by a linked process. Not reachable through safe_run/2, which
        %% converts every raise and throw itself.
        {ergon_down, ReqId, process, _Pid, Abnormal} ->
            {error, describe(Abnormal)};
        {ergon_spawn, ReqId, error, Reason} ->
            {error, describe({spawn_failed, Reason})}
    after Timeout ->
        %% Abandon before returning, so a reply already in flight is discarded
        %% rather than left in the mailbox for the next message to trip over.
        _ = spawn_request_abandon(ReqId),
        {error, ~"handler timeout"}
    end.

safe_run(Handler, Message) ->
    try Handler(Message) of
        ok -> ok;
        {error, Reason} when is_binary(Reason) -> {error, Reason};
        Other -> {error, describe({handler_returned, Other})}
    catch
        Class:Reason:Stacktrace ->
            {error, describe({Class, Reason, Stacktrace})}
    end.

describe(Term) ->
    iolist_to_binary(io_lib:format("~0p", [Term])).
