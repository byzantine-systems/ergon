-module(ergon_new_job).
-moduledoc """
The specification for a job that has not been inserted yet.

Build one with `new/1,2` and the setters rather than writing the map literally,
so adding a future tuning knob never breaks an existing call site:

```erlang
Job = ergon_new_job:unique_for(
        ergon_new_job:with_max_attempts(
          ergon_new_job:on_queue(
            ergon_new_job:new(~"send_email", #{~"to" => ~"a@b.com"}),
            ~"mailers"),
          5),
        60),
{ok, Inserted} = ergon:enqueue(Job).
```

No id, no state, no fingerprint: all three belong to the database. The
fingerprint in particular is a generated column over `(queue, worker, payload)`,
so it can never disagree with the row it identifies.
""".

-include_lib("ergon/include/ergon.hrl").

-export([
    new/1, new/2,
    on_queue/2,
    with_max_attempts/2,
    unique_for/2,
    dedup_seconds/1
]).

-export_type([new_job/0, uniqueness/0]).

-define(DEFAULT_QUEUE, ~"default").
-define(DEFAULT_MAX_ATTEMPTS, 20).

-doc "A job for `Worker` with an empty payload.".
-spec new(binary()) -> new_job().
new(Worker) -> new(Worker, #{}).

-doc """
Start building a job for `Worker` carrying `Payload`.

`Payload` is encoded as JSON on insert, so it must be a term `json:encode/1`
accepts. Defaults to the `default` queue, 20 attempts, and no uniqueness.
""".
-spec new(binary(), json:encode_value()) -> new_job().
new(Worker, Payload) when is_binary(Worker) ->
    #{
        queue => ?DEFAULT_QUEUE,
        worker => Worker,
        payload => Payload,
        max_attempts => ?DEFAULT_MAX_ATTEMPTS,
        uniqueness => not_unique
    }.

-doc "Place the job on a specific queue.".
-spec on_queue(new_job(), binary()) -> new_job().
on_queue(Job, Queue) when is_binary(Queue) ->
    Job#{queue := Queue}.

-doc "Set how many times the job may be attempted before it is marked failed.".
-spec with_max_attempts(new_job(), pos_integer()) -> new_job().
with_max_attempts(Job, MaxAttempts) when is_integer(MaxAttempts), MaxAttempts > 0 ->
    Job#{max_attempts := MaxAttempts}.

-doc """
Make the job unique for `Seconds`.

Enqueuing a second job with the same `(queue, worker, payload)` fingerprint
inside that window returns the incumbent instead of inserting a duplicate. The
window is a `dedup_period` separate from `valid_period`, so a unique job stays
checkoutable for its whole lifetime.
""".
-spec unique_for(new_job(), pos_integer()) -> new_job().
unique_for(Job, Seconds) when is_integer(Seconds), Seconds > 0 ->
    Job#{uniqueness := {unique_for, Seconds}}.

-doc """
The dedup window in seconds, as `ergon.enqueue` expects it.

Zero means non-unique, which the function turns into an empty `dedup_period`.
Empty ranges never overlap, so duplicates always insert.
""".
-spec dedup_seconds(new_job()) -> non_neg_integer().
dedup_seconds(#{uniqueness := not_unique}) -> 0;
dedup_seconds(#{uniqueness := {unique_for, Seconds}}) -> Seconds.
