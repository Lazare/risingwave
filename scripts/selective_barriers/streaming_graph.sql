-- Compact dump of the streaming job graph, one row per streaming job.
--
-- Columns:
--   id  job id
--   k   kind: t = table, mv = materialized view, sk = sink, i = index, so = shared source
--   f   number of fragments
--   a   number of actors
--   s   number of state tables (internal tables included)
--   d   data drivers: S = source connector, D = DML, N = now()
--   up  upstream job ids connected by a dispatcher edge, comma separated
--
-- Export with:
--   psql -h <host> -p 4566 -d <db> -U <user> -X -A -t -F '|' \
--        -f streaming_graph.sql > graph.psv
WITH frag AS (
    SELECT fragment_id,
           table_id AS job_id,
           upstream_fragment_ids,
           flags,
           cardinality(state_table_ids) AS n_states,
           parallelism
    FROM rw_fragments
),
actor_cnt AS (
    SELECT fragment_id, count(*) AS n
    FROM rw_actors
    GROUP BY fragment_id
),
edge AS (
    SELECT DISTINCT uf.job_id AS up_job, f.job_id AS down_job
    FROM frag f
    CROSS JOIN unnest(f.upstream_fragment_ids) AS u(up_fragment_id)
    JOIN frag uf ON uf.fragment_id = u.up_fragment_id
    WHERE uf.job_id <> f.job_id
),
job AS (
    SELECT f.job_id,
           count(*) AS frags,
           sum(coalesce(a.n, 0)) AS actors,
           sum(f.n_states) AS states,
           bool_or('SOURCE' = ANY(f.flags) OR 'SOURCE_SCAN' = ANY(f.flags) OR 'FS_FETCH' = ANY(f.flags)) AS has_source,
           bool_or('DML' = ANY(f.flags)) AS has_dml,
           bool_or('NOW' = ANY(f.flags)) AS has_now
    FROM frag f
    LEFT JOIN actor_cnt a ON a.fragment_id = f.fragment_id
    GROUP BY f.job_id
),
ups AS (
    SELECT down_job, array_agg(up_job ORDER BY up_job) AS up
    FROM edge
    GROUP BY down_job
)
SELECT j.job_id,
       CASE r.relation_type
           WHEN 'table' THEN 't'
           WHEN 'materialized view' THEN 'mv'
           WHEN 'sink' THEN 'sk'
           WHEN 'index' THEN 'i'
           WHEN 'source' THEN 'so'
           ELSE r.relation_type
       END,
       j.frags,
       j.actors,
       j.states,
       concat(CASE WHEN j.has_source THEN 'S' END,
              CASE WHEN j.has_dml THEN 'D' END,
              CASE WHEN j.has_now THEN 'N' END),
       coalesce(array_to_string(u.up, ','), '')
FROM job j
LEFT JOIN rw_relations r ON r.id = j.job_id
LEFT JOIN ups u ON u.down_job = j.job_id
ORDER BY j.job_id;
