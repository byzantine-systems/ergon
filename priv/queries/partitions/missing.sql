-- Months (current month through $1 months ahead, inclusive) that lack a
-- partition for parent table $2. Partitions follow the `<table>_YYYYMM`
-- naming convention installed by `Ergon.Migration.partitioned_table/2`.
-- Returns YYYYMM labels, empty when healthy.
-- $1: months ahead to verify
-- $2: parent table name
SELECT to_char(months.month_start, 'YYYYMM') AS missing_month
FROM
    generate_series(
        date_trunc('month', now()),
        date_trunc('month', now()) + make_interval(months => $1),
        INTERVAL '1 month'
    ) AS months (month_start)
WHERE
    to_regclass(
        $2 || '_' || to_char(months.month_start, 'YYYYMM')
    ) IS NULL;
