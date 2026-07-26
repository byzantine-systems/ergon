# High throughput with pgmq + Broadway

The SKIP LOCKED worker path (see [Getting started](getting-started.md)) is ideal
for jobs with real state and dependencies. For high-throughput streaming, where
you want backpressure, batching, and concurrency knobs, use
`Ergon.Pgmq.Producer` as a [Broadway](https://hexdocs.pm/broadway) producer over
a [pgmq](https://github.com/pgmq/pgmq) queue.

pgmq is the durable buffer; the producer polls it and wraps each message in a
`Broadway.Message`. The poll is the source of truth, a `LISTEN`/`NOTIFY`
fast-path is layered on top purely to cut latency.

## 1. Create the queue

In a migration, `import Ergon.Migration` and declare the queue:

```elixir
defmodule MyApp.Repo.Migrations.CreateTelemetryQueue do
  use Ecto.Migration
  import Ergon.Migration

  def change do
    pgmq_queue(:telemetry_processing)
  end
end
```

`pgmq_queue/1` is reversible: `down` drops the queue via `pgmq.drop_queue`.

## 2. Add the notify fast-path (optional but recommended)

`pgmq_notify_cron/2` installs a per-second pg_cron tick that fires
`pg_notify` whenever the queue holds a visible message, so the producer can wake
immediately instead of waiting out its poll interval:

```elixir
def change do
  pgmq_queue(:telemetry_processing)
  pgmq_notify_cron(:telemetry_processing)   # channel defaults to pgmq_telemetry_processing
end
```

This is deliberately a **cron tick, not an insert trigger**. A transaction with a
pending `NOTIFY` takes the global notification-queue lock at commit, so a
trigger would serialize every enqueuing commit, the LISTEN/NOTIFY scalability
trap. The tick is level-triggered: it re-notifies as long as deliverable work
exists, which also wakes consumers when a visibility timeout expires
(redelivery), something an insert trigger never did. See [Scheduling](scheduling.md).

Where pg_cron is absent, the schedule is a guarded no-op and the producer just
polls, still correct, only slower.

## 3. Use the producer in a Broadway pipeline

```elixir
defmodule MyApp.Pipeline do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {Ergon.Pgmq.Producer,
           queue: "telemetry_processing",
           poll_interval: 100,
           visibility_timeout: 30,
           notify_channel: "pgmq_telemetry_processing"},
        concurrency: 1
      ],
      processors: [default: [concurrency: 50]]
    )
  end

  @impl Broadway
  def handle_message(:default, %Broadway.Message{data: payload} = msg, _ctx) do
    MyApp.Telemetry.ingest(payload)
    msg
  end
end
```

### Producer options

| Option | Default | Meaning |
| --- | --- | --- |
| `:queue` | *(required)* | pgmq queue name. |
| `:repo` | `Ergon.Repo` | Ecto repo for pgmq ops; high-throughput apps should give it a separate pool. |
| `:poll_interval` | `100` | ms between polls while demand is unmet. |
| `:visibility_timeout` | `30` | seconds a read message stays hidden before redelivery. |
| `:notify_channel` | *(none)* | LISTEN channel for the wake-up fast-path; convention `pgmq_<queue>`. |

## Delivery semantics

Successfully processed messages are acked in batch via `pgmq.archive`. Failed
messages are left untouched, so their visibility timeout expires and pgmq
redelivers them, at-least-once delivery that survives BEAM crashes.

The producer is the source of truth; `LISTEN` is a latency optimisation only. A
dropped `NOTIFY` costs latency, never events: pgmq is the durable buffer, and the
poll loop keeps delivering regardless.

## A note on connection poolers

`LISTEN` needs a stable, dedicated backend connection. If your database URL
routes through a **transaction-mode** pooler (e.g. PgBouncer in `transaction`
mode), that connection still goes through the pooler and `LISTEN` will silently
never receive anything. Point `:repo`'s connection config at a session-mode route
when a transaction-mode pooler sits in front of Postgres.

## Recovering stranded messages

When a consumer dies mid-message, the visibility timeout eventually redelivers
it. To force recovery immediately, see [Operations](operations.md#disaster-recovery).
