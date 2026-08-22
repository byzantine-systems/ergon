-- FIFO read. Batches from the earliest group, SQS-style, favouring throughput over fairness.
--
-- Ordering is guaranteed WITHIN a group, identified by the x-pgmq-group header,
-- and messages in different groups proceed in parallel. A message sent without
-- that header joins a default group. Group ordering is upheld by the visibility
-- timeout: the next message in a group stays hidden until the one ahead of it is
-- archived or its lease expires.
-- $1: queue name
-- $2: visibility timeout (seconds)
-- $3: max messages
SELECT
    msg_id,
    read_ct,
    message,
    headers
FROM
    pgmq.read_grouped ($1, $2, $3);

