# Ergon examples

Task-oriented guides for leveraging Ergon. Each one is also published as a page
in the [generated docs](https://hexdocs.pm/ergon) under **Guides**.

Start here:

1. [Getting started](getting-started.md), the SKIP LOCKED worker path: enqueue
   jobs, run a worker, tune the queue.
2. [Unique jobs](unique-jobs.md), database-enforced deduplication with a
   temporal constraint.
3. [Workflows](workflows.md), DAG dependencies resolved by a SQL/PGQ property
   graph.

Going further:

4. [pgmq + Broadway](pgmq-broadway.md), high-throughput streaming with
   backpressure and at-least-once delivery.
5. [Migration helpers](migrations.md), reuse Ergon's PostgreSQL patterns:
   extensions, bi-temporal tables, graph tables, queues, partitions.
6. [Scheduling](scheduling.md), recurring SQL with pg_cron, and why Ergon
   prefers cron ticks to row triggers.
7. [Operations](operations.md), the job notifier, boot-time partition safety,
   health checks, and disaster recovery.

Every example is runnable against the setup in the [top-level
README](../README.md#quick-start).
