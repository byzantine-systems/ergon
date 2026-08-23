-- Like read, but blocks server-side until a message appears or $4 seconds pass,
-- checking every $5 milliseconds.
--
-- The alternative to the LISTEN/NOTIFY wake path, and a better one where a
-- consumer can afford a connection of its own: latency floor is $5 (default
-- 100ms) against the notify tick's 1s, and an idle queue costs no repeated
-- round-trips. The cost is that the connection is held for the whole call,
-- which is why ergon_pgmq_consumer gives a long-polling consumer its own
-- single-connection pool rather than letting it block the shared one.
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
    pgmq.read_with_poll ($1, $2, $3, $4, $5);

