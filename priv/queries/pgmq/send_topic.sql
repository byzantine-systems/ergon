-- Publish one message by routing key, fanning it out to every queue whose bound
-- pattern matches. Returns how many queues it was delivered to, which is zero
-- when nothing is bound: a topic send to an unmatched key is silently dropped,
-- not an error.
--
-- Routing is AMQP-style. `*` matches exactly one dot-separated segment and `#`
-- matches zero or more, so `logs.#` catches both `logs.error` and
-- `logs.api.error` while `logs.*` catches only the first. Routing keys
-- themselves may not contain wildcards.
--
-- The whole fan-out is one transaction: every delivery succeeds or none does.
-- $1: routing key
-- $2: message (jsonb)
-- $3: headers (jsonb, may be NULL)
SELECT
    pgmq.send_topic ($1, $2::jsonb, $3::jsonb, 0) AS delivered_to;

