-- Liveness probe for a repo/pool. Cheap, no table dependencies.
-- Used by `Ergon.Health.check/0` and boot checks.
SELECT 1 AS ok;
