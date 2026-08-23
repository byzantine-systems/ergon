-- Read up to $3 messages, hiding each behind a visibility timeout of $2
-- seconds. A message not archived before the timeout expires becomes visible
-- again and is redelivered: the at-least-once guarantee the consumer relies on.
--
-- Every read variant in this directory projects the same four columns, so
-- ergon_pgmq decodes one shape regardless of strategy. headers carries
-- x-pgmq-group for FIFO consumers and is NULL when the sender set none.
-- $1: queue name
-- $2: visibility timeout (seconds)
-- $3: max messages
SELECT
    msg_id,
    read_ct,
    message,
    headers
FROM
    pgmq.read ($1, $2, $3);

