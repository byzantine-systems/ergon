# Operations: health, notifier, boot safety, recovery

This guide covers the pieces you wire into a running node's supervision tree and
lean on in production: the reactive job notifier, boot-time partition safety,
health probes, and disaster recovery.

## The job notifier

`Ergon.JobNotifier` turns `ergon.jobs` into a reactive queue. One instance per
node opens a dedicated auto-reconnecting `LISTEN` connection on the channel
`ergon_job_available` and, on each notification, wakes exactly the workers
draining the named queue, instead of making them wait out their poll interval.

The emitting half is the pg_cron tick installed by
`Ergon.Migration.job_notify_cron/0` (shipped by Ergon's initial migration):
`ergon.notify_pending_jobs()` runs every second and fires
`pg_notify('ergon_job_available', queue)` once per queue holding an immediately
runnable job. The payload is the **queue name only**, never job data or tenant,
because `NOTIFY` bypasses RLS and is visible to any session on the database.

The poll stays the durable path. `ergon.jobs` is the durable fact and checkout's
`FOR UPDATE SKIP LOCKED` is the reliable puller, so a `NOTIFY` is a hint, not an
event that can be lost. A dropped notification costs latency, never a stuck job,
the fallback poll ([`Ergon.Queue`](getting-started.md#tuning-the-queue)) covers
the boot gap, reconnect windows, and every pg_cron-less environment.

Disable it and workers simply poll, still fully correct, only slower:

```elixir
config :ergon, Ergon.JobNotifier, enabled: false
```

> **Connection poolers:** `LISTEN` needs a stable, dedicated backend connection.
> If your database URL routes through a transaction-mode pooler (e.g. PgBouncer
> in `transaction` mode), that connection still goes through the pooler and
> `LISTEN` silently receives nothing. Point the connection config at a
> session-mode route in that case.

## Boot-time partition safety

For partitioned tables (see [Migrations](migrations.md#partition-lifecycle)), add
`Ergon.PartitionBootCheck` to your supervision tree. It runs in `init/1` and
blocks the rest of the tree from starting until the next N months of partitions
are verified, so a node never boots into a state where it can't write the
current month's data:

```elixir
children = [
  {Ergon.PartitionBootCheck, table: :asset_telemetry_pings, months_ahead: 2}
]
```

This is the boot-time complement to the weekly pg_cron lifecycle job: cron keeps
the horizon ahead during normal operation; the boot check guarantees it before
the app accepts traffic.

## Health

`Ergon.Health.check/1` returns a flat map suitable for a `/health` endpoint: DB
liveness, installed extension versions, and per-queue metrics.

```elixir
config :ergon, queues: ~w(telemetry_processing geofence_alerts)

Ergon.Health.check()
#=> %{
#     db: {:ok, %Postgrex.Result{rows: [[1]]}},
#     extensions: %{"pgmq" => "1.5.0", "btree_gist" => "1.5", ...},
#     queues: %{
#       "telemetry_processing" => %{queue_length: 5, queue_visible_length: 3, ...}
#     }
#   }
```

The queue list comes from `config :ergon, queues: [...]`, or pass
`queues: [...]` to override it for a single call. Hosts with no pgmq queues can
leave it unset, `:queues` defaults to an empty map.

## Disaster recovery

When a consumer dies mid-message, the pgmq visibility timeout eventually
redelivers it. To force recovery immediately, `Ergon.Reconciler.run/1` reconciles
the runtime state layer with the durable store in two moves: it invokes your
`:hydrate` callback (where you stop and rebuild in-memory state), then
force-releases every in-flight visibility lease per queue so stranded messages
become immediately deliverable.

```elixir
# Host with an in-memory state holder:
Ergon.Reconciler.run(
  queues: ~w(telemetry_processing geofence_alerts),
  hydrate: &MyApp.StateTracker.stop_all_and_rebuild/0
)

# Host with no in-memory state, pgmq-only recovery:
Ergon.Reconciler.run(queues: ~w(jobs))
```

It returns a summary map with the callback's result and a per-queue breakdown
(released leases and queue metrics). Run it from `mix ergon.reconcile` (when
shipped) or directly from a remote console on a running node.
