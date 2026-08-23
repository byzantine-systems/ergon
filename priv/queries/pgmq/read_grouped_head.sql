-- FIFO read. At most one message per group, so no group is ever processed out of order. Uses FOR UPDATE SKIP LOCKED, so distinct consumers take distinct groups.
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
    pgmq.read_grouped_head ($1, $2, $3);

