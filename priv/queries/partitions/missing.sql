-- Months (current month through $1 months ahead, inclusive) that lack a
-- partition for parent table $2. Partitions follow the `<table>_YYYYMM`
-- naming convention installed by `Ergon.Migration.partitioned_table/2`.
-- Returns YYYYMM labels, empty when healthy.
-- $1: months ahead to verify
-- $2: parent table name
SELECT
    TO_CHAR(months.month_start, 'YYYYMM') AS missing_month
FROM
    GENERATE_SERIES(DATE_TRUNC('month', NOW()), DATE_TRUNC('month', NOW()) + MAKE_INTERVAL(months => $1), INTERVAL '1 month') AS months (month_start)
WHERE
    TO_REGCLASS($2 || '_' || TO_CHAR(months.month_start, 'YYYYMM')) IS NULL;

