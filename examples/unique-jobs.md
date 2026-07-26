# Unique jobs

Ergon deduplicates jobs in the database, not the application. A unique job hashes
its `(queue, worker, payload)` into a `fingerprint`, and duplicates collide on
the temporal constraint:

```sql
UNIQUE (fingerprint, valid_period WITHOUT OVERLAPS)
```

Because the uniqueness window is *temporal*, "unique" always means "unique for a
period of time" rather than "unique forever". That maps cleanly onto the real
requirement, one welcome email per user per hour, one nightly report per day,
without a separate dedup table or a Redis lock.

## Making a job unique

Add `unique_for/2` with the window length in seconds:

```elixir
Ergon.NewJob.new("nightly_report", %{date: Date.utc_today()})
|> Ergon.NewJob.unique_for(3600)   # one delivery per hour
|> Ergon.enqueue()
```

Enqueuing a second job with the same fingerprint inside that window is rejected
by PostgreSQL's temporal unique constraint, the deduplication is enforced by the
database, so it holds even across nodes and concurrent enqueues racing the same
instant.

## The default is *not* unique

A non-unique job (the default from `Ergon.NewJob.new/2`) salts its fingerprint
with random bytes, so two otherwise-identical enqueues never collide:

```elixir
# These are two distinct jobs, even with identical payloads:
Ergon.NewJob.new("send_email", %{to: "alice@example.com"}) |> Ergon.enqueue()
Ergon.NewJob.new("send_email", %{to: "alice@example.com"}) |> Ergon.enqueue()
```

Modelling uniqueness as `:not_unique | {:unique_for, seconds}` (rather than a
boolean plus a nullable duration) makes the invalid states, "unique but no
window", "not unique but has a window", unrepresentable.

## Choosing the window

The fingerprint covers the payload, so put the deduplication key *in* the
payload. Two enqueues collide only when queue, worker, **and** payload all match.

```elixir
# Deduplicated per user per day: include the date in the payload.
Ergon.NewJob.new("daily_digest", %{user_id: 42, date: Date.utc_today()})
|> Ergon.NewJob.unique_for(86_400)
|> Ergon.enqueue()
```

If you leave the volatile part out of the payload, every enqueue in the window
collapses to one job; if you include it, each distinct value is its own job.
