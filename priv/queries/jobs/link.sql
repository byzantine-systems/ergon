-- Record a dependency edge in the workflow graph. Re-adding an existing edge
-- is a no-op.
INSERT INTO ergon.job_edges (parent_id, child_id, edge_type)
VALUES ($1, $2, $3)
ON CONFLICT (parent_id, child_id) DO NOTHING;
