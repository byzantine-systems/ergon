-- Installed extensions and their versions. Backs the health endpoint's
-- `:extensions` field and environment diagnostics.
SELECT
    extname,
    extversion
FROM pg_extension
ORDER BY extname;
