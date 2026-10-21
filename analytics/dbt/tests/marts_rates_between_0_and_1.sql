-- Every rate of the quality marts is a proportion from 0 to 1, equal to numerator / denominator, and only proportion
-- measures have one.
WITH cells AS (
    SELECT 'monthly' AS mart, measure_id, source, unit, numerator, denominator, rate, is_suppressed
    FROM {{ ref('mart_quality_monthly') }}
    UNION ALL
    SELECT 'period', measure_id, source, unit, numerator, denominator, rate, is_suppressed
    FROM {{ ref('mart_quality_period') }}
)
SELECT *
FROM cells
WHERE (rate IS NOT NULL AND (rate < 0 OR rate > 1 OR unit <> 'proportion'
                             OR ABS(rate - numerator / denominator) > 0.00005))
   OR (unit = 'proportion' AND NOT is_suppressed AND rate IS NULL)
