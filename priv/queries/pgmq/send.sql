-- Send one message to a queue, optionally with headers. Returns its msg_id.
--
-- Headers are how FIFO grouping is expressed: x-pgmq-group names the group a
-- message belongs to, and the grouped read strategies keep strict order within
-- each. Pass NULL for no headers.
-- $1: queue name
-- $2: message (jsonb)
-- $3: headers (jsonb, may be NULL)
SELECT
    pgmq.send ($1, $2::jsonb, $3::jsonb) AS msg_id;

