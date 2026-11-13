-- Which streaming jobs write to Hummock at (almost) every checkpoint, based on the
-- version deltas still kept by the meta node (about the last minute).
--
-- Output, one row per job (compact, no names):
--   job   job id (matches streaming_graph.sql)
--   tbl   number of its state tables that wrote at least once
--   pct   share of checkpoints in which the job wrote (100 = every checkpoint)
--
-- The last row (job = -1) gives: tbl = number of checkpoints seen,
-- pct = average number of distinct SST objects uploaded per checkpoint.
WITH recent AS (
    SELECT id, group_deltas
    FROM rw_hummock_version_deltas
    WHERE id > (SELECT max(id) - 300 FROM rw_hummock_version_deltas)
),
sst AS (
    SELECT id,
           jsonb_path_query(group_deltas, '$.*.groupDeltas[*].newL0SubLevel.insertedTableInfos[*]') AS info
    FROM recent
),
ckpt AS (
    SELECT count(DISTINCT id) AS n FROM sst
),
w AS (
    SELECT DISTINCT s.id, t.table_id
    FROM sst s
    CROSS JOIN LATERAL (
        SELECT jsonb_array_elements_text(s.info -> 'tableIds')::int AS table_id
    ) t
),
per_job AS (
    SELECT coalesce(it.job_id, w.table_id) AS job,
           count(DISTINCT w.table_id) AS tbl,
           count(DISTINCT w.id) AS commits
    FROM w
    LEFT JOIN rw_internal_tables it ON it.id = w.table_id
    GROUP BY 1
)
SELECT job, tbl, round(100.0 * commits / (SELECT n FROM ckpt)) AS pct
FROM per_job
UNION ALL
SELECT -1,
       (SELECT n FROM ckpt),
       round((SELECT count(DISTINCT info ->> 'objectId') FROM sst)::numeric / greatest((SELECT n FROM ckpt), 1), 1)
ORDER BY 3 DESC, 1;
