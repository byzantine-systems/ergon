-- Every job id reachable from $1 through 'triggers' edges: the transitive
-- closure a cascade (e.g. cancellation) of $1 would touch. A recursive CTE over
-- ergon.job_edges, PostgreSQL 19's SQL/PGQ does not yet support path
-- quantifiers, so multi-hop reachability lives here rather than in GRAPH_TABLE.
-- $1: ancestor job id
WITH RECURSIVE
    descendants (id) AS (
        SELECT child_id FROM ergon.job_edges
        WHERE parent_id = $1
        UNION
        SELECT edges.child_id
        FROM ergon.job_edges AS edges
            JOIN descendants ON edges.parent_id = descendants.id
    )

SELECT id FROM descendants;
