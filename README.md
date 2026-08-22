# Ergon

[![Built with Nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)
[![[Nix] Build & Test](https://github.com/byzantine-systems/ergon/actions/workflows/build.yml/badge.svg)](https://github.com/byzantine-systems/ergon/actions/workflows/build.yml)
![License](https://img.shields.io/github/license/byzantine-systems/ergon)

> [!WARNING]
> This project is under active development. Avoid using it for production apps.

A library for PostgreSQL-native background job and workflow processing in Erlang/OTP, with the simple premise that a recent PostgreSQL is enough on its own, so:

- No Redis.
- No separate graph database.
- No third-party job DSL.

Everything leans on database capabilities:

- [Temporal tables](https://www.postgresql.org/docs/19/ddl-temporal-tables.html) and [temporal constraints](https://neon.com/postgresql/18/temporal-constraints): unique jobs enforced by a temporal primary key, auditable history via `FOR PORTION OF` updates rather than in-place overwrites.
- **`UPDATE ... FOR PORTION OF`** (see [updating and deleting temporal data](https://www.postgresql.org/docs/19/dml-application-time-update-delete.html)), which closes a job's validity window and writes a fresh row for the new state.
- SQL/PGQ [property graphs](https://www.postgresql.org/docs/19/ddl-property-graphs.html): workflow dependencies resolved with a `GRAPH_TABLE`/`MATCH` query.

Ergon also ships generic infrastructure for the common PostgreSQL stack: **pgmq** durable queues with topics, FIFO groups and long polling, **pg_cron** guarded schedule helpers, and monthly **partition lifecycle** management.

> [!NOTE]
> Ergon targets **PostgreSQL 19** and **OTP 28** (see `flake.nix`). Both are pinned: `GRAPH_TABLE` and `FOR PORTION OF` are PG19 features, and the source uses OTP 28 syntax.

## Quick start

### 1. Add the dependency

```erlang
%% rebar.config
{deps, [{ergon, "0.5.0"}]}.
```

### 2. Point it at a database

Connection settings come from the standard `libpq` environment at boot: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`. Nothing else is required, though `config/sys.config` can tune the pool and the wake paths.

### 3. Install the schema

```erlang
{ok, _Summary} = ergon_migrate:migrate().
```

Ergon owns its own schema, in `priv/migrations/`, applied by `migraterl` under the `ergon` namespace. Host migrations can be registered alongside it, see [migration helpers](examples/migrations.md).

### 4. Enqueue and Work

```erlang
{ok, Job} = ergon:enqueue(
              ergon_new_job:on_queue(
                ergon_new_job:new(~"send_email", #{~"to" => ~"a@b.com"}),
                ~"mailers")),

{ok, _Worker} = ergon:start_worker(
                  ergon_queue:with_concurrency(ergon_queue:new(~"mailers"), 4),
                  fun(#{payload := #{~"to" := To}}) ->
                      my_mailer:send(To)
                  end).
```

A handler returns `ok` to complete the job, or `{error, Reason}` to record the reason and retry until attempts are spent. Anything else, and any raise or throw, is treated as an error, so one bad job never takes a worker down.

## What the database does

Almost everything, which is the point:

| Concern | Where it lives |
|---|---|
| Uniqueness | a partial `EXCLUDE USING gist` over a temporal dedup window |
| Retry backoff | jittered capped exponential, in `ergon.retry_backoff` |
| Legal state transitions | the `jobs_transition_guard` trigger |
| History | `FOR PORTION OF` splits plus a system-time history twin |
| Workflow blocking | `pending_parents`, inside the fetch index's predicate |
| Fair, contention-free checkout | `FOR UPDATE SKIP LOCKED` |

The Erlang side is the thin part: a poller, an executor pool, and a `LISTEN` connection.

## Transactional enqueue

Because a job is a row, enqueuing inside your own transaction makes the job and the data that justifies it commit or roll back together:

```erlang
ergon_repo:transaction(fun() ->
    {ok, _} = ergon_repo:query("UPDATE orders SET status = 'paid' WHERE id = $1", [OrderId]),
    {ok, _} = ergon:enqueue(ergon_new_job:new(~"send_receipt", #{~"order" => OrderId}))
end).
```

This is the transactional outbox pattern without the outbox. There is no window in which the order was marked paid but the job was lost, and none in which the job runs for an update that rolled back.

## Guides

Task-oriented walkthroughs live in [`examples/`](examples/README.md).

## Development

```bash
# devenv shell: PG19 with pg_cron and pgmq, OTP 28
nix develop --impure

# start postgres
devenv up
rebar3 compile
rebar3 dialyzer

# nixfmt, erlfmt and pg_format
nix fmt
```
