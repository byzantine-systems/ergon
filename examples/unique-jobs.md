# Unique jobs

Deduplication enforced by the database, not by application bookkeeping.

## The problem

Two requests arrive at once and both enqueue "send the welcome email to user 42". Checking for an existing job first does not help: two processes can both check, both see nothing, and both insert. Closing that race in application code means a lock, and a lock means somewhere to put it.

## The mechanism

```erlang
Job = ergon_new_job:unique_for(
    ergon_new_job:new(~"welcome_email", #{~"user" => 42}),
    60
),

{ok, First}  = ergon:enqueue(Job),
{ok, Second} = ergon:enqueue(Job),

%% same job, inserted once
true = maps:get(id, First) =:= maps:get(id, Second).
```

The second call is not an error. It returns the **incumbent**, which is usually what a caller wants: "make sure this work is queued" rather than "insert a row".

Three pieces make that work, and it is worth being precise about them because they are easy to describe wrongly.

### The fingerprint is deterministic and unsalted

`ergon.jobs.fingerprint` is a generated column:

```sql
fingerprint text GENERATED ALWAYS AS (
    encode(digest(length(queue)::text || ':' || queue || ':' ||
                  length(worker)::text || ':' || worker || ':' ||
                  payload::text, 'sha256'), 'hex')) STORED
```

Generated in the database so it can never disagree with the columns it summarises, and length-prefixed so `(queue, worker)` pairs cannot be confused by concatenation. **Nothing is salted.** Two jobs with the same queue, worker and payload always have the same fingerprint, whether or not either is unique.

### Uniqueness comes from the dedup window

What differs between a unique job and a non-unique one is `dedup_period`:

- `unique_for(Job, N)` gives a bounded window, `[now, now + N seconds)`.
- A non-unique job gets `'empty'`.

Empty ranges never overlap anything, including other empty ranges. So duplicates of a non-unique job coexist happily while sharing a fingerprint.

### The constraint is a partial EXCLUDE

```sql
CONSTRAINT jobs_unique_fingerprint
EXCLUDE USING gist (
    (coalesce(tenant, '')) WITH =,
    fingerprint WITH =,
    dedup_period WITH &&)
WHERE (upper(valid_period) = 'infinity')
```

Not a `UNIQUE` constraint: PostgreSQL cannot express "these are equal and those overlap" as one, which is exactly what temporal deduplication needs.

The `WHERE` clause is important. Without it the constraint would collide with `FOR PORTION OF`: a state transition leaves a superseded row carrying the same fingerprint and the same `dedup_period`, and the split would conflict with itself. Restricting to live rows lets history accumulate freely.

`coalesce(tenant, '')` scopes uniqueness per tenant while still applying to rows with no tenant.

## Why the window is separate from validity

A job has two periods, and conflating them would break one or the other:

- `valid_period` is when the row is true in the world. It runs to `infinity` until a state transition closes it.
- `dedup_period` is only about uniqueness.

If uniqueness were keyed on `valid_period`, a unique job would stop being checkoutable the moment its dedup window expired, because the two would be the same range. Keeping them separate means a unique job stays runnable for its whole life while only being *undeduplicated* after its window closes.

## How the incumbent is returned

`ergon.enqueue` catches the exclusion violation and selects the live overlapping row instead:

```sql
-- (...)
EXCEPTION
    WHEN exclusion_violation THEN
        RETURN QUERY
        SELECT * FROM ergon.jobs
        WHERE fingerprint = ... AND upper(valid_period) = 'infinity';
```

Which is why the second `enqueue` returns a job rather than an error, and why that job carries the first one's id.

## Choosing a window

The window is how long "the same job" means "already queued". Too short and a retry storm enqueues duplicates, too long and a genuinely new request is swallowed by an old one.

A reasonable rule: at least as long as the work takes, plus the retry budget. Size the budget on the *worst* case rather than the typical one, because backoff is jittered and the typical case is about half of it. For a job with 20 attempts on the defaults the ceilings add up to a little under 24 minutes, of which any given run will spend around 12; a window shorter than the full 24 can admit a duplicate while the original is still retrying.
