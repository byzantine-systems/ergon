-- Expire every in-flight visibility lease on a queue, making the messages
-- deliverable immediately. Recovery tool for messages stranded by consumers
-- that died mid-processing, instead of waiting out their visibility
-- timeouts, the reconciler frees them all. Returns the number of leases
-- released.
-- $1: queue name
SELECT pgmq_release_leases($1) AS released;
