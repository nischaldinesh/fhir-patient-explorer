-- Small cell suppression in both quality marts: no count of 1 to 10 is shown (nor one that the rate and denominator
-- would give back), a suppressed cell shows no count, rate or value at all, and a shown cell shows its denominator.
WITH cells AS (
    SELECT 'monthly' AS mart, measure_id, source, unit, numerator, denominator, rate, value, is_suppressed
    FROM {{ ref('mart_quality_monthly') }}
    UNION ALL
    SELECT 'period', measure_id, source, unit, numerator, denominator, rate, value, is_suppressed
    FROM {{ ref('mart_quality_period') }}
)
SELECT *
FROM cells
WHERE denominator BETWEEN 1 AND 10
   OR (unit = 'proportion' AND (numerator BETWEEN 1 AND 10 OR denominator - numerator BETWEEN 1 AND 10))
   OR (is_suppressed AND (numerator IS NOT NULL OR denominator IS NOT NULL OR rate IS NOT NULL OR value IS NOT NULL))
   OR (NOT is_suppressed AND denominator IS NULL)
