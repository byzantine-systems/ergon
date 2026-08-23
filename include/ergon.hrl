%% Ergon's shared type vocabulary.
%%
%% Every value that crosses a module boundary is a map with atom keys, and its
%% shape is declared exactly once, here. Modules include this header and
%% `-export_type` the types they own, so a host application can write
%% `ergon_job:job()` in its own specs without including anything, while Ergon's
%% own modules refer to the bare names.
%%
%% Two types are NOT declared here, and cannot be: the nominals `job_id()` and
%% `attempt()`. Both are non-negative integers that travel as positional
%% arguments, which is exactly where a port swaps them, so they are nominal
%% (EEP-69) to make dialyzer reject the swap. Integer literals still typecheck;
%% only a value already typed as the other nominal is refused.
%%
%% Nominal identity is per-module, so declaring one in a header included
%% everywhere mints a separate, mutually incompatible type in every module that
%% includes it, and every cross-module call then looks like the very mistake the
%% nominal was meant to catch. They live in `ergon_job` instead and are referred
%% to by qualified name below and in every spec.

%% ---------------------------------------------------------------------------
%% PostgreSQL wire values
%% ---------------------------------------------------------------------------

%% What `pg_types` hands back for a timestamptz column, and what it accepts on
%% the way in. Always UTC. Seconds carry sub-second precision as a float, so the
%% field is `number()` rather than `0..59`. `pg_timestampz` hardcodes this
%% representation, ignoring the `timestamp_config` that plain `timestamp`
%% honours, so there is no integer-epoch alternative for any Ergon column.
-type pg_timestamp() ::
    {calendar:date(), {0..23, 0..59, number()}}
    | infinity
    | '-infinity'.

%% SQL NULL. `pgo` uses the atom `null` in both directions, `undefined` is not
%% a NULL to the driver and would be encoded as an atom.
-type pg_null() :: null.

%% A `tstzrange` as `pg_range` decodes it, before `ergon_temporal_period`
%% converts it. Infinite endpoints arrive as `unbound`.
-type pg_range() ::
    empty
    | {{pg_timestamp() | unbound, pg_timestamp() | unbound}, {boolean(), boolean()}}.

%% ---------------------------------------------------------------------------
%% Jobs
%% ---------------------------------------------------------------------------

%% Mirrors the `ergon.job_state` domain. The database owns the set, this is the
%% decoded form.
-type job_state() :: available | executing | completed | failed | discarded.

%% A fully materialised job row, decoded from a query projected with
%% `ergon_job:column_list/0`. `payload` is decoded JSON, not the raw text the
%% column projects. The wire form the driver hands `ergon_job:from_row/1` is an
%% eleven-element tuple, not a list, whatever `pgo:row()` claims.
-type job() :: #{
    id := ergon_job:job_id(),
    queue := binary(),
    worker := binary(),
    payload := json:decode_value(),
    state := job_state(),
    fingerprint := binary(),
    attempt := ergon_job:attempt(),
    max_attempts := pos_integer(),
    last_error := binary() | pg_null(),
    scheduled_at := pg_timestamp(),
    inserted_at := pg_timestamp()
}.

%% Whether Ergon should prevent duplicate jobs, and for how long. A tagged value
%% rather than a boolean plus a nullable duration, so "unique but no window" and
%% "not unique but has a window" are unrepresentable.
-type uniqueness() :: not_unique | {unique_for, pos_integer()}.

%% The specification for a job that has not been inserted yet. No id, no state,
%% no fingerprint: all three are the database's to assign.
-type new_job() :: #{
    queue := binary(),
    worker := binary(),
    payload := json:encode_value(),
    max_attempts := pos_integer(),
    uniqueness := uniqueness()
}.

%% A host-supplied job handler. `ok` completes the job, `{error, Reason}` records
%% the reason and retries until attempts are spent. Anything else, and any raise
%% or throw, is treated as an error so one bad job never takes a worker down.
-type handler() :: fun((job()) -> ok | {error, binary()}).

%% Runtime configuration for a single named queue.
%%
%% `batch_size` and `concurrency` are different knobs and easy to conflate.
%% `batch_size` is how many jobs one checkout round-trip fetches, `concurrency`
%% is how many of that queue's handlers may run at once, and it sizes the
%% executor pool. A poll never fetches more than the pool has free capacity for,
%% so a checked-out job is never left sitting `executing` in a mailbox.
-type queue() :: #{
    name := binary(),
    poll_interval := pos_integer(),
    batch_size := pos_integer(),
    concurrency := pos_integer(),
    handler_timeout := timeout()
}.

%% ---------------------------------------------------------------------------
%% pgmq
%%
%% The other queue: a durable message transport, unrelated to ergon.jobs. These
%% shapes come from pgmq's own functions rather than from an Ergon table.
%% ---------------------------------------------------------------------------

%% One message as pgmq.read returns it. `message` is decoded from the jsonb
%% column rather than handed over as raw JSON text. `read_ct` counts deliveries
%% and is the only signal a handler has for detecting a poison message, since
%% pgmq has no dead-letter concept.
-type pgmq_message() :: #{
    id := non_neg_integer(),
    read_ct := non_neg_integer(),
    message := json:decode_value(),
    headers := json:decode_value() | pg_null()
}.

%% How a consumer takes messages off a queue.
%%
%% `plain` takes whatever is visible. The grouped strategies order strictly
%% within each `x-pgmq-group` while letting different groups proceed in
%% parallel: `grouped_head` takes at most one message per group, `grouped_rr`
%% round-robins so no group starves another, `grouped` batches from the earliest
%% group for throughput.
%%
%% The `long_poll` forms block server-side until a message arrives or MaxSeconds
%% elapses, checking every IntervalMs. They hold their connection for the
%% duration, which is why a long-polling consumer gets a pool of its own.
-type pgmq_read_strategy() ::
    plain
    | grouped
    | grouped_head
    | grouped_rr
    | {long_poll, MaxSeconds :: pos_integer(), IntervalMs :: pos_integer()}
    | {long_poll, plain | grouped | grouped_head | grouped_rr, pos_integer(), pos_integer()}.

-type pgmq_topic_binding() :: #{
    pattern := binary(),
    queue_name := binary(),
    bound_at := pg_timestamp(),
    compiled_regex := binary()
}.

%% The gap between the two lengths is messages hidden behind a visibility lease,
%% either in flight or stranded by a dead consumer. `oldest_msg_age_sec` is null
%% on an empty queue.
-type pgmq_metrics() :: #{
    queue_length := non_neg_integer(),
    queue_visible_length := non_neg_integer(),
    oldest_msg_age_sec := number() | pg_null()
}.

%% A host-supplied message handler. Same contract as handler(): `ok` archives the
%% message, `{error, Reason}` leaves it for its visibility timeout to redeliver.
-type pgmq_handler() :: fun((pgmq_message()) -> ok | {error, binary()}).

%% Runtime configuration for one pgmq consumer.
%%
%% `handler_timeout` is `undefined` until set, and resolves to the visibility
%% timeout rather than to infinity as queue() does. See
%% ergon_pgmq_queue:effective_handler_timeout/1. Past the visibility timeout the
%% message is deliverable again and may already be in someone else's hands, so
%% continuing is pointless and archiving afterwards would remove work another
%% consumer is still doing.
-type pgmq_queue() :: #{
    name := binary(),
    poll_interval := pos_integer(),
    batch_size := pos_integer(),
    visibility_timeout := pos_integer(),
    concurrency := pos_integer(),
    handler_timeout := timeout() | undefined,
    notify_channel := binary() | undefined,
    read_strategy := pgmq_read_strategy()
}.

%% ---------------------------------------------------------------------------
%% Lifecycle state machine
%% ---------------------------------------------------------------------------

%% Something that has happened to a job and may move it to a new state.
%%
%%   fetched        a worker checked the job out to run it
%%   succeeded      the handler returned successfully
%%   {errored, Why} the handler returned an error carrying Why
%%   cancelled      the job was cancelled before completing
-type fsm_event() :: fetched | succeeded | {errored, binary()} | cancelled.

%% The state to persist after a transition, with its attempt count and last
%% error. `ergon_db:apply_outcome/2` is what writes it.
-type fsm_outcome() :: #{
    state := job_state(),
    attempt := ergon_job:attempt(),
    last_error := binary() | pg_null()
}.

%% How `ergon.retry_backoff` turns an attempt count into a delay, configured
%% under `{ergon_db, [{retry_backoff, ...}]}`. The names are the article's, and
%% `none` is its unjittered baseline; see the function for the formulas and the
%% citation. Its fourth variant, Decorrelated Jitter, is absent because it needs
%% the previous delay carried across attempts, which for a database-backed queue
%% means a column.
-type retry_backoff_strategy() :: full_jitter | equal_jitter | none.

%% Returned when an event does not make sense for a job's current state.
-type invalid_transition() :: #{from := job_state(), event := fsm_event()}.

%% ---------------------------------------------------------------------------
%% Temporal periods
%% ---------------------------------------------------------------------------

%% PostgreSQL has no scalar PERIOD type, application- and system-time periods
%% are `tstzrange` values. This is the decoded, type-safe form. An infinite
%% endpoint is `unbounded`, an empty range is `empty := true` with `empty`
%% endpoints.
-type period_endpoint() :: pg_timestamp() | unbounded | empty.

-type temporal_period() :: #{
    lower := period_endpoint(),
    upper := period_endpoint(),
    lower_inclusive := boolean(),
    upper_inclusive := boolean(),
    empty := boolean()
}.

%% ---------------------------------------------------------------------------
%% SQL loader
%% ---------------------------------------------------------------------------

%% priv/queries/jobs/insert.sql -> {jobs, insert}
-type sql_key() :: {atom(), atom()}.

-type sql_root() ::
    file:filename_all()
    | {priv, atom(), file:filename_all()}
    | {app, atom(), file:filename_all()}.

-type sql_option() ::
    {root, file:filename_all()}
    | {extra_roots, [sql_root()]}.

%% ---------------------------------------------------------------------------
%% Data access
%% ---------------------------------------------------------------------------

%% `pgo:query/3` answers a bare map on success, `ergon_repo` normalises that to
%% `{ok, _}` so callers can pattern match uniformly.
-type repo_result() :: {ok, pgo:result()} | {error, term()}.

%% Per-query driver options, threaded from `ergon_sql:query/3` down to
%% `pgo:query/3`. Spelled out here because the driver keeps its own `options()`
%% type unexported, and an opaque `map()` would let a typo through.
-type query_options() :: #{
    pool => atom(),
    trace => boolean(),
    include_statement_span_attribute => boolean(),
    queue => boolean(),
    decode_opts => list(),
    pool_options => list()
}.

-type db_error() ::
    empty_result
    | would_create_cycle
    | {job_not_found, ergon_job:job_id()}
    | {pgo_error, map()}
    | term().
