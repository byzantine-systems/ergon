-- Ack a batch of processed messages: pgmq.archive moves them from
-- pgmq.q_<queue> to the pgmq.a_<queue> audit table, so a processed message
-- leaves a durable trail instead of vanishing. Returns the ids actually
-- archived (already-archived ids are silently absent).
-- The ::bigint[] cast is load-bearing: pgmq.archive is overloaded on
-- (text, bigint) and (text, bigint[]), so an untyped parameter fails to
-- resolve at prepare time.
-- $1: queue name
-- $2: message ids
SELECT archived.msg_id
FROM pgmq.archive($1, $2::bigint []) AS archived (msg_id);
