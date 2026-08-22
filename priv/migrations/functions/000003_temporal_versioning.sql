-- Shared, generic system-time versioning trigger function.
-- Class: on_change (ergon_migrate:sources/1). Applies before schema/,
-- because jobs_versioning_trigger there attaches it.
--
-- Column-agnostic: the function inspects tg_table_name/tg_table_schema at
-- fire time and archives the OLD row into <schema>.<table>_history by naming
-- convention, so it serves host tables in any schema, not just ergon's. That
-- generality is in the body, not in where the function lives.
--
-- Any table wanting bi-temporal versioning attaches it with:
--
--   CREATE TRIGGER <table>_versioning_trigger
--     BEFORE INSERT OR UPDATE OR DELETE ON <table>
--     FOR EACH ROW EXECUTE FUNCTION ergon.temporal_versioning();
--
-- ## The instant: statement_timestamp(), not clock_timestamp()
--
-- Belief time has to advance *within* a transaction, because two beliefs really
-- did happen in sequence. That rules out now()/transaction_timestamp(), which is
-- frozen transaction-wide: an insert followed by an update inside one
-- transaction would close the archived row at its own lower bound and produce an
-- empty, and therefore invisible, system_time window.
--
-- statement_timestamp() advances between statements while staying fixed within
-- one, which is exactly the granularity belief time wants, and it is STABLE
-- rather than VOLATILE (verify with `SELECT provolatile FROM pg_proc WHERE
-- proname = 'statement_timestamp'`). Two consequences, both wanted:
--
--   * Nothing here is opaque to the planner. A VOLATILE call must be
--     re-evaluated per row and cannot be folded, which is what makes one
--     unwelcome anywhere near a scan; a STABLE one is evaluated once and behaves
--     like a parameter.
--
--   * One statement produces one belief instant. clock_timestamp() advances on
--     every call, so an UPDATE touching a thousand rows would stamp a thousand
--     belief instants microseconds apart, and the archived upper bound would not
--     even meet the live lower bound on the *same* row: two calls, two values, a
--     gap between the versions where an as-of query finds nothing at all. A
--     single statement is a single change in belief, and this records it as one.
--
-- `at` is still captured once rather than called twice. With a STABLE function
-- both calls would agree, so this is for the reader: the moment the old belief
-- ends is the same moment the new one begins, and the code should say so.
--
-- ## The open bound
--
-- Always the literal 'infinity', never NULL. Both spell "forever" to a range
-- containment test, but they are different values and only one of them is a
-- value: upper_inf() separates them, `= 'infinity'` finds one and not the other,
-- and a table mixing the two cannot be queried consistently for liveness. Ergon
-- writes 'infinity' everywhere, which is what valid_period already keys off in
-- the is_live generated column and in jobs_fetch_idx.
--
-- Explicitly `ergon.`, never bare: every object Ergon owns lives in the ergon
-- schema, and a bare name would land wherever the connecting role's
-- search_path happens to point.
CREATE OR REPLACE FUNCTION ergon.temporal_versioning ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
DECLARE
    at timestamptz := STATEMENT_TIMESTAMP();
BEGIN
    IF tg_op IN ('UPDATE', 'DELETE') THEN
        OLD.system_time := TSTZRANGE(LOWER(OLD.system_time), at, '[)');
        EXECUTE FORMAT('INSERT INTO %I.%I SELECT ($1).*', tg_table_schema, tg_table_name || '_history')
        USING old;
    END IF;
        IF tg_op = 'DELETE' THEN
            RETURN old;
        END IF;
        NEW.system_time := TSTZRANGE(at, 'infinity', '[)');
        RETURN new;
END
$$;

