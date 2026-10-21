-- mart_suppression_report counts every cell of both quality marts once, and its suppressed counts match them.
WITH report AS (
    SELECT mart, SUM(cells) AS cells, SUM(suppressed) AS suppressed
    FROM {{ ref('mart_suppression_report') }} GROUP BY mart
),
actual AS (
    SELECT 'mart_quality_monthly' AS mart, COUNT(*) AS cells, SUM(CASE WHEN is_suppressed THEN 1 ELSE 0 END) AS suppressed
    FROM {{ ref('mart_quality_monthly') }}
    UNION ALL
    SELECT 'mart_quality_period', COUNT(*), SUM(CASE WHEN is_suppressed THEN 1 ELSE 0 END)
    FROM {{ ref('mart_quality_period') }}
)
SELECT a.mart, a.cells, r.cells AS report_cells, a.suppressed, r.suppressed AS report_suppressed
FROM actual a
LEFT JOIN report r ON r.mart = a.mart
WHERE a.cells > 0 AND (r.mart IS NULL OR r.cells <> a.cells OR r.suppressed <> a.suppressed)
