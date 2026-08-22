-module(ergon_pgmq).
-moduledoc """
Thin wrappers over pgmq, plus the queue and notification management a host needs.

pgmq is Ergon's *other* queue, and it is worth being clear about which is which.
`ergon.jobs` is the bi-temporal job table: retries, workflow dependencies,
history, one row per job. pgmq is a durable message transport: a host creates
its own queues on it and streams through them at volume, and delivery is
at-least-once by virtue of visibility timeouts rather than state transitions.
Nothing here touches `ergon.jobs`.

`ergon_pgmq_consumer` is the read side; `ergon_reconciler` and `ergon_health`
(Phase 6) are the operational side. Every query lives in `priv/queries/pgmq/`
and runs through `ergon_sql`, so nothing here builds SQL, except the
`*_sql/1,2` forms at the bottom, which exist precisely to hand SQL to a host's
own migration.

## Delivery contract

`read/3` hides each message behind a visibility timeout. A message not archived
before that timeout expires becomes visible again and is redelivered. So
**archive means acknowledge**, and a message must be archived only after its
handler succeeded. Failing to archive is how a failure is reported; there is no
negative acknowledgement to send.

`read_ct` counts how many times a message has been delivered. Nothing here
dead-letters a message that keeps failing, since pgmq has no such concept, so a
handler that wants to give up on a poison message must check `read_ct` itself.

## Sending inside your own transaction

`send/2,3` and `send_topic/2,3` are ordinary queries on Ergon's pool, so calling
one inside `ergon_repo:transaction/1` enlists it in that transaction: `pgo`
binds the transaction's connection in the process dictionary and every nested
query rides it. That makes a message and the rows that justify it commit or roll
back together:

```erlang
ergon_repo:transaction(fun() ->
    {ok, _} = ergon_repo:query("UPDATE accounts SET balance = balance - $1 WHERE id = $2",
                               [Amount, From]),
    {ok, _} = ergon_pgmq:send(~"payments", #{~"from" => From, ~"amount" => Amount})
end).
```

This is the transactional outbox pattern with no outbox: there is no window in
which the balance moved but the message was lost, and none in which the message
went out for a transfer that rolled back. The usual machinery (an outbox table,
a poller, a reconciler for drift) exists to close a gap that does not open when
the queue lives in the same database as the data. The same holds for
`ergon:enqueue/1`.
""".

-include_lib("ergon/include/ergon.hrl").

-export([
    read/3, read/4, read/5,
    send/2, send/3, send/4,
    archive/2, archive/3,
    metrics/1, metrics/2,
    release_leases/1, release_leases/2,
    group_header/1
]).

-export([
    bind_topic/2,
    unbind_topic/2,
    send_topic/2, send_topic/3, send_topic/4,
    list_topic_bindings/1
]).

-export([
    create_queue/1,
    drop_queue/1,
    enable_notify/1, enable_notify/2,
    disable_notify/1,
    default_channel/1
]).

-export([
    create_queue_sql/1,
    drop_queue_sql/1,
    enable_notify_sql/1, enable_notify_sql/2,
    disable_notify_sql/1
]).

-export_type([pgmq_message/0, pgmq_metrics/0, pgmq_read_strategy/0, pgmq_topic_binding/0]).

%% ---------------
%% Reading and acknowledging
%% ---------------

-doc """
Read up to `Limit` messages from `Queue`, hiding each behind a visibility
timeout of `VtSeconds`.

`message` and `headers` are `jsonb`, and the driver's default json configuration
returns those as raw binaries. Both are decoded here, so a handler receives
terms, not JSON text.
""".
-spec read(binary(), pos_integer(), pos_integer()) ->
    {ok, [pgmq_message()]} | {error, db_error()}.
read(Queue, VtSeconds, Limit) -> read(Queue, VtSeconds, Limit, plain).

-doc """
Like `read/3` with an explicit strategy.

`plain` takes whatever is visible, in no particular order. The three grouped
strategies deliver in strict order **within** each group named by the
`x-pgmq-group` header, while letting different groups proceed in parallel:

- `grouped_head` takes at most one message per group, so no group can ever be
  worked out of order. `FOR UPDATE SKIP LOCKED` underneath, so distinct
  consumers take distinct groups.
- `grouped_rr` round-robins across groups, so a busy group cannot starve others.
- `grouped` batches from the earliest group, favouring throughput over fairness.

Ordering is enforced by the visibility timeout rather than by locking: the next
message in a group stays hidden until the one ahead is archived or its lease
expires. A message sent without the header joins a default group.

The `{long_poll, MaxSeconds, IntervalMs}` strategy modifier blocks server-side
until a message arrives. See `read/5`.
""".
-spec read(binary(), pos_integer(), pos_integer(), pgmq_read_strategy() | query_options()) ->
    {ok, [pgmq_message()]} | {error, db_error()}.
read(Queue, VtSeconds, Limit, Opts) when is_map(Opts) ->
    read(Queue, VtSeconds, Limit, plain, Opts);
read(Queue, VtSeconds, Limit, Strategy) ->
    read(Queue, VtSeconds, Limit, Strategy, #{}).

-doc """
Like `read/4` with driver options, which is how a long-polling consumer pins the
call to its own connection pool.

That pinning is not optional for `{long_poll, _, _}`: the call blocks
server-side for up to `MaxSeconds`, holding its connection the whole time, so
issuing it against the shared pool would take a connection out of circulation
for every other query on the node.
""".
-spec read(
    binary(), pos_integer(), pos_integer(), pgmq_read_strategy(), query_options()
) ->
    {ok, [pgmq_message()]} | {error, db_error()}.
read(Queue, VtSeconds, Limit, Strategy, Opts) when
    is_binary(Queue), is_integer(VtSeconds), is_integer(Limit)
->
    {Key, Extra} = read_key(Strategy),
    maybe
        {ok, #{rows := Rows}} ?=
            ergon_sql:query(Key, [Queue, VtSeconds, Limit | Extra], Opts),
        {ok, [to_message(Row) || Row <- Rows]}
    end.

read_key(plain) ->
    {{pgmq, read}, []};
read_key(grouped) ->
    {{pgmq, read_grouped}, []};
read_key(grouped_head) ->
    {{pgmq, read_grouped_head}, []};
read_key(grouped_rr) ->
    {{pgmq, read_grouped_rr}, []};
read_key({long_poll, Max, Interval}) ->
    {{pgmq, read_with_poll}, [Max, Interval]};
read_key({long_poll, grouped, Max, Interval}) ->
    {{pgmq, read_grouped_with_poll}, [Max, Interval]};
read_key({long_poll, grouped_head, Max, Interval}) ->
    {{pgmq, read_grouped_head_with_poll}, [Max, Interval]};
read_key({long_poll, grouped_rr, Max, Interval}) ->
    {{pgmq, read_grouped_rr_with_poll}, [Max, Interval]};
read_key({long_poll, plain, Max, Interval}) ->
    {{pgmq, read_with_poll}, [Max, Interval]}.

%% ---------------
%% Sending
%% ---------------

-doc "Send `Message` to `Queue`. Returns its message id.".
-spec send(binary(), json:encode_value()) -> {ok, non_neg_integer()} | {error, db_error()}.
send(Queue, Message) -> send(Queue, Message, null).

-doc """
Like `send/2` with headers.

Headers are how FIFO grouping is expressed: `group_header/1` builds the
`x-pgmq-group` header the grouped read strategies order by.
""".
-spec send(binary(), json:encode_value(), json:encode_value() | pg_null()) ->
    {ok, non_neg_integer()} | {error, db_error()}.
send(Queue, Message, Headers) -> send(Queue, Message, Headers, #{}).

-spec send(binary(), json:encode_value(), json:encode_value() | pg_null(), query_options()) ->
    {ok, non_neg_integer()} | {error, db_error()}.
send(Queue, Message, Headers, Opts) when is_binary(Queue) ->
    Params = [Queue, json:encode(Message), encode_headers(Headers)],
    maybe
        {ok, #{rows := [{MsgId}]}} ?= ergon_sql:query({pgmq, send}, Params, Opts),
        {ok, MsgId}
    end.

-doc """
The header that places a message in a FIFO group.

```erlang
ergon_pgmq:send(Queue, Payload, ergon_pgmq:group_header(~"customer-42"))
```
""".
-spec group_header(binary()) -> #{binary() => binary()}.
group_header(GroupId) when is_binary(GroupId) -> #{~"x-pgmq-group" => GroupId}.

-doc """
Acknowledge `MsgIds` on `Queue`.

`pgmq.archive` moves them from `pgmq.q_<queue>` to the `pgmq.a_<queue>` audit
table, so a processed message leaves a durable trail rather than vanishing.
Returns the ids actually archived; an id already archived is silently absent.

An empty list short-circuits without a round-trip. That is not just an
optimisation: it is the normal case for a batch in which every handler failed,
and sending an empty array would cost a query per cycle to archive nothing.
""".
-spec archive(binary(), [non_neg_integer()]) -> {ok, [non_neg_integer()]} | {error, db_error()}.
archive(Queue, MsgIds) -> archive(Queue, MsgIds, #{}).

-spec archive(binary(), [non_neg_integer()], query_options()) ->
    {ok, [non_neg_integer()]} | {error, db_error()}.
archive(_Queue, [], _Opts) ->
    {ok, []};
archive(Queue, MsgIds, Opts) when is_binary(Queue), is_list(MsgIds) ->
    maybe
        {ok, #{rows := Rows}} ?= ergon_sql:query({pgmq, archive}, [Queue, MsgIds], Opts),
        {ok, [Id || {Id} <:- Rows]}
    end.

-doc """
Health snapshot of one queue: total length, visible length, and the age of the
oldest message.

The difference between the two lengths is messages hidden behind a visibility
lease: in flight, or stranded by a consumer that died. `oldest_msg_age_sec` is
`null` on an empty queue.

`queue_visible_length` is computed against transaction-frozen `now()`, so a
message sent inside the same transaction reads as invisible. Assert on
`queue_length` in transactional tests.
""".
-spec metrics(binary()) -> {ok, pgmq_metrics()} | {error, db_error()}.
metrics(Queue) -> metrics(Queue, #{}).

-spec metrics(binary(), query_options()) -> {ok, pgmq_metrics()} | {error, db_error()}.
metrics(Queue, Opts) when is_binary(Queue) ->
    maybe
        {ok, #{rows := [{Length, Visible, Oldest} | _]}} ?=
            ergon_sql:query({pgmq, metrics}, [Queue], Opts),
        {ok, #{
            queue_length => Length,
            queue_visible_length => Visible,
            oldest_msg_age_sec => Oldest
        }}
    end.

-doc """
Force-expire every in-flight visibility lease on `Queue`, making the held
messages immediately re-readable. Returns how many were released.

The recovery tool for messages stranded by consumers that died mid-processing:
rather than waiting out each visibility timeout individually, free them all at
once. `ergon_reconciler` is the intended caller.
""".
-spec release_leases(binary()) -> {ok, non_neg_integer()} | {error, db_error()}.
release_leases(Queue) -> release_leases(Queue, #{}).

-spec release_leases(binary(), query_options()) -> {ok, non_neg_integer()} | {error, db_error()}.
release_leases(Queue, Opts) when is_binary(Queue) ->
    maybe
        {ok, #{rows := [{Released}]}} ?=
            ergon_sql:query({pgmq, release_leases}, [Queue], Opts),
        {ok, Released}
    end.

%% ---------------
%% Queue and notification management
%% ---------------

-doc "Create a pgmq queue and its archive table.".
-spec create_queue(binary()) -> ok | {error, db_error()}.
create_queue(Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    maybe
        {ok, _} ?= ergon_sql:query({pgmq, create_queue}, [Name]),
        ok
    end.

-doc "Drop a pgmq queue, its archive table, and any notification registration.".
-spec drop_queue(binary()) -> ok | {error, db_error()}.
drop_queue(Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    maybe
        ok ?= disable_notify(Name),
        {ok, _} ?= ergon_sql:query({pgmq, drop_queue}, [Name]),
        ok
    end.

-doc "The channel `Queue` is notified on by default: `pgmq_<queue>`.".
-spec default_channel(binary()) -> binary().
default_channel(Queue) -> <<"pgmq_", Queue/binary>>.

-doc """
Register `Queue` for wake-up notifications on its default channel.

This installs nothing. Ergon's migrations already schedule a single
`ergon.notify_pending_pgmq()` tick that reads the registry every second and
notifies each registered queue holding a visible message.

One tick rather than one cron job per queue, because every notifying transaction
takes the global notification-queue lock at commit. That is the cost that makes
trigger-per-insert designs plateau, measured at ~2.9K writes/sec with no
resource saturation, against ~60K once notifications were batched into fewer
transactions. A per-queue tick would put the notifying-transaction count back in
proportion to the queue count; this keeps it at one per second regardless.

Wake latency floor is therefore the tick's 1 s cadence, pg_cron's finest
granularity. Consumers that need lower latency should lower `poll_interval`
instead; the poll is the durable path in either case.
""".
-spec enable_notify(binary()) -> ok | {error, db_error()}.
enable_notify(Queue) -> enable_notify(Queue, default_channel(Queue)).

-doc "Like `enable_notify/1` with an explicit channel. Re-registering moves it.".
-spec enable_notify(binary(), binary()) -> ok | {error, db_error()}.
enable_notify(Queue, Channel) ->
    Name = ergon_ident:validate(Queue, queue),
    Chan = ergon_ident:validate(Channel, channel),
    maybe
        {ok, _} ?= ergon_sql:query({pgmq, enable_notify}, [Name, Chan]),
        ok
    end.

-doc "Stop notifying for `Queue`. Its consumers fall back to polling.".
-spec disable_notify(binary()) -> ok | {error, db_error()}.
disable_notify(Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    maybe
        {ok, _} ?= ergon_sql:query({pgmq, disable_notify}, [Name]),
        ok
    end.

%% ---------------
%% Topics
%% ---------------
%%
%% AMQP-style routing on top of ordinary queues: bind patterns to queues, then
%% publish by routing key and let pgmq fan the message out to everything that
%% matches. Nothing here changes how a queue is consumed: a topic-delivered
%% message is an ordinary message in an ordinary queue.

-doc """
Bind a routing pattern to a queue.

`*` matches exactly one dot-separated segment and `#` matches zero or more, so
`logs.#` catches both `logs.error` and `logs.api.error` while `logs.*` catches
only the first. Idempotent: rebinding the same pattern to the same queue is a
no-op.
""".
-spec bind_topic(binary(), binary()) -> ok | {error, db_error()}.
bind_topic(Pattern, Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    maybe
        {ok, _} ?= ergon_sql:query({pgmq, bind_topic}, [Pattern, Name]),
        ok
    end.

-doc "Remove a binding. Answers whether one was actually removed.".
-spec unbind_topic(binary(), binary()) -> {ok, boolean()} | {error, db_error()}.
unbind_topic(Pattern, Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    maybe
        {ok, #{rows := [{Unbound}]}} ?=
            ergon_sql:query({pgmq, unbind_topic}, [Pattern, Name]),
        {ok, Unbound}
    end.

-doc """
Publish by routing key, fanning out to every queue whose bound pattern matches.
Answers how many queues received it.

**Zero is not an error.** A routing key nothing is bound to is silently dropped,
which is the behaviour a topic exchange is supposed to have but is also an easy
way to lose messages to a typo. Check the count, or `list_topic_bindings/1`,
if delivery matters.

The fan-out is one transaction: every delivery succeeds or none does.
""".
-spec send_topic(binary(), json:encode_value()) ->
    {ok, non_neg_integer()} | {error, db_error()}.
send_topic(RoutingKey, Message) -> send_topic(RoutingKey, Message, null).

-doc "Like `send_topic/2` with headers, e.g. a FIFO group.".
-spec send_topic(binary(), json:encode_value(), json:encode_value() | pg_null()) ->
    {ok, non_neg_integer()} | {error, db_error()}.
send_topic(RoutingKey, Message, Headers) -> send_topic(RoutingKey, Message, Headers, #{}).

-spec send_topic(
    binary(), json:encode_value(), json:encode_value() | pg_null(), query_options()
) ->
    {ok, non_neg_integer()} | {error, db_error()}.
send_topic(RoutingKey, Message, Headers, Opts) when is_binary(RoutingKey) ->
    Params = [RoutingKey, json:encode(Message), encode_headers(Headers)],
    maybe
        {ok, #{rows := [{Delivered}]}} ?= ergon_sql:query({pgmq, send_topic}, Params, Opts),
        {ok, Delivered}
    end.

-doc """
Every pattern bound to `Queue`.

`compiled_regex` is what pgmq actually matches routing keys against, which is
the thing to look at when a binding is not catching what its author expected.
""".
-spec list_topic_bindings(binary()) -> {ok, [pgmq_topic_binding()]} | {error, db_error()}.
list_topic_bindings(Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    maybe
        {ok, #{rows := Rows}} ?= ergon_sql:query({pgmq, list_topic_bindings}, [Name]),
        {ok, [
            #{pattern => P, queue_name => Q, bound_at => At, compiled_regex => Re}
         || {P, Q, At, Re} <:- Rows
        ]}
    end.

%% ---------------
%% SQL-only forms
%% ---------------
%%
%% For hosts that would rather put queue setup in their own migration than call
%% the executing forms at boot. These are the one place in Ergon that builds SQL
%% outside priv/queries, which is the whole point of them; every name is passed
%% through ergon_ident:validate/2 first, so the literals below cannot carry
%% anything but [a-z0-9_].

-doc "The statement `create_queue/1` runs, as literal SQL.".
-spec create_queue_sql(binary()) -> iodata().
create_queue_sql(Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    ["SELECT pgmq.create('", Name, "')"].

-doc "The statement `drop_queue/1` runs, as literal SQL.".
-spec drop_queue_sql(binary()) -> iodata().
drop_queue_sql(Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    ["SELECT pgmq.drop_queue('", Name, "')"].

-doc "The statement `enable_notify/1` runs, as literal SQL.".
-spec enable_notify_sql(binary()) -> iodata().
enable_notify_sql(Queue) -> enable_notify_sql(Queue, default_channel(Queue)).

-doc "The statement `enable_notify/2` runs, as literal SQL.".
-spec enable_notify_sql(binary(), binary()) -> iodata().
enable_notify_sql(Queue, Channel) ->
    Name = ergon_ident:validate(Queue, queue),
    Chan = ergon_ident:validate(Channel, channel),
    [
        "INSERT INTO ergon.pgmq_notify_queues (queue_name, channel) VALUES ('",
        Name,
        "', '",
        Chan,
        "') ON CONFLICT (queue_name) DO UPDATE SET channel = excluded.channel"
    ].

-doc "The statement `disable_notify/1` runs, as literal SQL.".
-spec disable_notify_sql(binary()) -> iodata().
disable_notify_sql(Queue) ->
    Name = ergon_ident:validate(Queue, queue),
    ["DELETE FROM ergon.pgmq_notify_queues WHERE queue_name = '", Name, "'"].

%% ---------------
%% Helpers
%% ---------------

to_message({MsgId, ReadCt, Message, Headers}) ->
    #{
        id => MsgId,
        read_ct => ReadCt,
        message => json:decode(Message),
        headers => decode_headers(Headers)
    }.

%% A message sent without headers has a NULL column, which is not the same as an
%% empty object and should not be flattened into one: a handler distinguishing
%% "no headers" from "empty headers" is entitled to.
decode_headers(null) -> null;
decode_headers(Headers) -> json:decode(Headers).

encode_headers(null) -> null;
encode_headers(Headers) -> json:encode(Headers).
