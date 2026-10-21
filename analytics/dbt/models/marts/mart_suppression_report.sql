-- Mart suppression report. Grain: one row per quality mart and grain (period type, and by source or all sources): how
-- many cells there are, how many are suppressed, and the share. Shows at a glance which grain a dashboard can use.
WITH cells AS (
    SELECT 'mart_quality_monthly' AS mart, 'month by source' AS grain, 1 AS grain_order, is_suppressed,
           FALSE AS is_complementary
    FROM {{ ref('mart_quality_monthly') }}
    UNION ALL
    SELECT 'mart_quality_period',
           REPLACE(period_type, '_', ' ') || CASE WHEN source = 'all' THEN ' all sources' ELSE ' by source' END,
           CASE period_type WHEN 'quarter' THEN 2 WHEN 'year' THEN 4 ELSE 6 END
               + CASE WHEN source = 'all' THEN 1 ELSE 0 END,
           is_suppressed,
           CASE WHEN suppression_reason = 'complementary' THEN TRUE ELSE FALSE END
    FROM {{ ref('mart_quality_period') }}
)
SELECT
    mart,
    grain,
    CAST(MIN(grain_order) AS INTEGER)                                               AS grain_order,
    CAST(COUNT(*) AS BIGINT)                                                        AS cells,
    CAST(SUM(CASE WHEN is_suppressed THEN 1 ELSE 0 END) AS BIGINT)                  AS suppressed,
    CAST(SUM(CASE WHEN is_complementary THEN 1 ELSE 0 END) AS BIGINT)               AS suppressed_complementary,
    CAST(ROUND(100.0 * SUM(CASE WHEN is_suppressed THEN 1 ELSE 0 END) / COUNT(*), 1) AS DOUBLE) AS pct_suppressed
FROM cells
GROUP BY mart, grain
