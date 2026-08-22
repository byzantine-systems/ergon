-- Serialize concurrent edge writes for the duration of the calling transaction.
--
-- ergon_db:link/3 does a reachability check and then an insert. Those two are
-- atomic inside a transaction, but atomicity alone does not make them correct:
-- under READ COMMITTED two concurrent links (A->B and B->A) each evaluate their
-- check against a snapshot taken before the other's insert, both see no cycle,
-- and both commit, closing exactly the loop the check exists to prevent.
--
-- A transaction-scoped advisory lock on a fixed key makes the check-and-insert
-- pair mutually exclusive, which is what actually closes the race. The lock is
-- released at commit or rollback, so there is nothing to clean up. Edges are
-- written rarely, so the contention cost is negligible, and the checkout path
-- never takes this lock.
--
-- hashtextextended returns bigint directly, matching pg_advisory_xact_lock's
-- single-argument form.
--
-- The IS NOT NULL is not a test, it is a cast. pg_advisory_xact_lock returns
-- void, and no pg_types codec claims void's typsend, so projecting the bare
-- call hands the driver an unknown OID carrying an empty binary and its decoder
-- raises a function_clause. Comparing forces a boolean column, which decodes
-- normally. Only the side effect matters; the value is always true.
SELECT
    PG_ADVISORY_XACT_LOCK(HASHTEXTEXTENDED('ergon.job_edges', 0)) IS NOT NULL AS locked;

