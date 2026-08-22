# Examples

Task-oriented guides. Each is also published as a page in the [generated docs](https://hexdocs.pm/ergon) under **Guides**.

Start here:

1. [Getting started](getting-started.md), the worker path: enqueue jobs, run a worker, tune the queue, enqueue inside your own transaction.
2. [Unique jobs](unique-jobs.md), database-enforced deduplication with a temporal constraint.
3. [Workflows](workflows.md), DAG dependencies that actually block, resolved by a SQL/PGQ property graph.

Going further:

4. [pgmq](pgmq.md), durable message streaming: batch delivery, topics, FIFO groups, and three ways to wake a consumer.
5. [Migration helpers](migrations.md), reuse Ergon's PostgreSQL patterns for your own tables, and register your migrations with Ergon's runner.
6. [Scheduling](scheduling.md), recurring SQL with pg_cron, and why Ergon prefers cron ticks to row triggers.
7. [Operations](operations.md), health checks, disaster recovery, derived-state drift, and boot-time partition safety.

Every example is runnable against the setup in the [top-level README](../README.md#quick-start).
