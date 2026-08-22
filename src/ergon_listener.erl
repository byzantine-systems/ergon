-module(ergon_listener).
-moduledoc """
The node's single `LISTEN` connection, shared by everything that wants
notifications.

`LISTEN` needs a stable, dedicated backend: it cannot ride the query pool,
because a subscription belongs to a session. Ergon has two kinds of subscriber:
`ergon_job_notifier` on `ergon_job_available`, and one `ergon_pgmq_consumer` per
pgmq queue with a `notify_channel`. Giving each its own connection would mean
`1 + N` dedicated backends per node. The driver registers subscriptions per
channel *per calling process*, so one connection serves them all instead.

## Why this is a module of functions and not a proxy

`pgo_notifications:listen/2` monitors its caller and delivers notifications to
it. A subscriber therefore has to make that call from its own process. Routing
it through a `gen_server` here would subscribe *this* process and send every
notification to the wrong place. So the connection is a supervised child under a
registered name, and `subscribe/1` is a plain function the caller runs itself.

## Reconnection, and the one case it does not cover

The connection auto-reconnects and re-issues `LISTEN` for every channel it knows
about when it comes back, so an ordinary database blip needs no help: the
subscription survives and messages resume. What it cannot survive is the driver
process itself crashing, which loses the subscription table with it. `subscribe/1`
therefore monitors the connection as well as listening, and subscribers
re-subscribe on `DOWN`.

Either way this is only the fast path. Both kinds of subscriber poll on their own
interval regardless, so a lost subscription costs latency, never work.
""".

-export([
    child_spec/0,
    name/0,
    subscribe/1,
    unsubscribe/1
]).

-include_lib("kernel/include/logger.hrl").

-define(NAME, ergon_pg_notifications).

-doc """
Child spec for the shared connection.

An `ergon_sup` child, ahead of every subscriber. Its configuration is
`ergon_repo:pool_config/0`, the same `PG*` environment as the query pool,
because a listener that reached a different database would silently receive
nothing.
""".
-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?NAME,
        start => {pgo_notifications, start_link, [{local, ?NAME}, ergon_repo:pool_config()]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [pgo_notifications]
    }.

-doc "The registered name of the shared connection.".
-spec name() -> atom().
name() -> ?NAME.

-doc """
Subscribe the **calling process** to `Channel`.

Returns the subscription reference and a monitor reference. Notifications arrive
as `{notification, Pid, SubRef, Channel, Payload}`; a `{'DOWN', MonRef, ...}`
means the connection died and the caller should re-subscribe.

Two shapes of reply are possible and both are success: `{ok, Ref}` when the
connection is already up, `{eventually, Ref}` when it is not and the `LISTEN`
will land on connect. The reply is taken apart with `element/2` rather than
matched, because the driver specs this function as returning `{ok, reference()}`
only. Matching the `eventually` shape against that spec is a dialyzer error even
though the code plainly returns it from its disconnected state. Both shapes carry
the reference second.
""".
-spec subscribe(binary()) -> {ok, reference(), reference()}.
subscribe(Channel) when is_binary(Channel) ->
    MonRef = erlang:monitor(process, whereis(?NAME)),
    Reply = pgo_notifications:listen(?NAME, Channel),
    ?LOG_INFO(#{at => listening, channel => Channel, status => element(1, Reply)}),
    {ok, element(2, Reply), MonRef}.

-doc "Drop a subscription taken out by `subscribe/1`.".
-spec unsubscribe(reference()) -> ok.
unsubscribe(SubRef) ->
    pgo_notifications:unlisten(?NAME, SubRef).
