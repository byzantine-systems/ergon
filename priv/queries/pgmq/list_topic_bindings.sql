-- Every pattern bound to $1, for introspection. The compiled regex is what
-- pgmq actually matches routing keys against, and is useful when a binding is
-- not catching what its author expected.
-- $1: queue name
SELECT
    pattern,
    queue_name,
    bound_at,
    compiled_regex
FROM
    pgmq.list_topic_bindings ($1);

