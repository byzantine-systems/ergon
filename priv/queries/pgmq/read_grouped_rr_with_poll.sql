-- FIFO read with server-side long polling. Round-robin across groups, so one busy group cannot starve the others.
--
-- See read_with_poll.sql for why this needs a connection of its own, and
-- read_grouped_rr.sql for the grouping semantics.
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
    pgmq.read_grouped_rr_with_poll ($1, $2, $3, $4, $5);

