-- Read up to $3 messages from a pgmq queue, hiding each one behind a
-- visibility timeout of $2 seconds. A message that is not archived before
-- the timeout expires becomes visible again and is redelivered, this is
-- the at-least-once guarantee the Broadway pipeline relies on.
-- $1: queue name
-- $2: visibility timeout (seconds)
-- $3: max messages
SELECT
    msg_id,
    read_ct,
    message
FROM pgmq.read($1, $2, $3);
