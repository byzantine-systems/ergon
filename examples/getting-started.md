# Getting started: the SKIP LOCKED worker path

The simplest way to use Ergon is the job table plus a polling worker. You
enqueue jobs, start one worker per queue, and Ergon handles checkout, execution,
retries, and state transitions. Nothing beyond a recent PostgreSQL is involved.

This guide assumes you've already added the dependency, configured `Ergon.Repo`,
and run `mix ecto.setup` (see the [README](../README.md#quick-start)).

## Enqueue a job

`Ergon.NewJob` is a pipe-friendly builder. You give it a *worker* name (the kind
of work) and a JSON-encodable *payload*, then enqueue it:

```elixir
{:ok, job} =
  Ergon.NewJob.new("send_email", %{to: "alice@example.com", body: "Welcome!"})
  |> Ergon.NewJob.on_queue("mailers")
  |> Ergon.enqueue()
```

`new/2` defaults to the `default` queue, 20 attempts, and no uniqueness. Every
setter returns the struct, so you only reach for the knobs you need:

```elixir
Ergon.NewJob.new("send_email", %{to: "alice@example.com"})
|> Ergon.NewJob.on_queue("mailers")
|> Ergon.NewJob.with_max_attempts(5)
|> Ergon.enqueue()
```

## Run a worker

A worker is a supervised process that drains one queue. Each poll it checks out a
batch with `FOR UPDATE SKIP LOCKED`, runs your handler on each job, and persists
the resulting state transition.

```elixir
def handle(%Ergon.Job{payload: json}) do
  %{to: to, body: body} = Jason.decode!(json, keys: :atoms)
  MyApp.Mailer.send(to, body)
  :ok
end

{:ok, _worker} =
  Ergon.Queue.new("mailers")
  |> Ergon.start_worker(&handle/1)
```

The handler's return value drives the state machine:

| Return | Effect |
| --- | --- |
| `:ok` | Job is completed. |
| `{:error, reason}` | `reason` is recorded and the job is retried until `max_attempts` is exhausted, after which it is marked failed. |

## Tuning the queue

`Ergon.Queue` carries the per-worker runtime knobs. The default `poll_interval`
is 5 s and the default `batch_size` is 1:

```elixir
Ergon.Queue.new("mailers")
|> Ergon.Queue.with_poll_interval(1_000)  # poll every second
|> Ergon.Queue.with_batch_size(10)        # check out up to 10 jobs per poll
|> Ergon.start_worker(&handle/1)
```

### Why 5 s is the *default*, not the whole story

The poll is a fallback, not the primary wake path. With `Ergon.JobNotifier`
running and pg_cron installed, a runnable job wakes its workers over
`LISTEN`/`NOTIFY` within the notifier's ~1 s tick, so the poll only covers the
boot gap and reconnect windows. See [Operations](operations.md) for the
notifier, and [Scheduling](scheduling.md) for how the pg_cron tick is installed.

Where pg_cron is absent (no `NOTIFY` ever fires) the poll is the *only* wake
path, which is why the default stays moderate rather than long. Raise it on
pg_cron-backed deployments; lower it if the notifier is disabled and you need
tight latency from polling alone.

## Next steps

- [Unique jobs](unique-jobs.md), deduplicate enqueues with a temporal constraint.
- [Workflows](workflows.md), wire up DAG dependencies between jobs.
- [Operations](operations.md), health checks, the job notifier, and recovery.
