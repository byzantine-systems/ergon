-- FIFO read with server-side long polling. At most one message per group, so no group is ever processed out of order. Uses FOR UPDATE SKIP LOCKED, so distinct consumers take distinct groups.
--
-- See read_with_poll.sql for why this needs a connection of its own, and
-- read_grouped_head.sql for the grouping semantics.
-- $1: queue name
-- $2: visibility timeout (seconds)
-- $3: max messages
-- $4: max poll seconds
-- $5: poll interval (milliseconds)
SELECT
    msg_id,
    read_ct,
    message,
    headers
FROM
    pgmq.read_grouped_head_with_poll ($1, $2, $3, $4, $5);

