-- Defense-in-depth transition guard. Class: on_change
-- (ergon_migrate:sources/1). Applies before schema/, because
-- jobs_transition_guard there attaches it as a BEFORE UPDATE trigger.
--
-- Rejects any illegal state transition at write time regardless of caller,
-- while ergon_fsm stays the client-side fast path. The legal edges below
-- mirror ergon_fsm:transition/2 exactly; change one and you must change the
-- other.
--
-- Referenced only by a trigger on ergon.jobs, and its body touches no ergon
-- table, so it is creatable before the table exists.
CREATE OR REPLACE FUNCTION ergon.enforce_job_transition ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.state IS DISTINCT FROM OLD.state AND (OLD.state,
        NEW.state)
    NOT IN (('available', 'executing'), ('executing', 'completed'), ('executing', 'failed'), ('executing', 'available'), ('available', 'discarded'), ('executing', 'discarded')) THEN
        RAISE EXCEPTION 'illegal job transition: % -> %', OLD.state, NEW.state
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$;

