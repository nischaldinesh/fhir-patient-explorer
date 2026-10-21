-- Internal: every quality measure aggregated, before any suppression, for each period (month, quarter, year, all_time)
-- and source (mimic, synthea, hl7v2, and all: the sources pooled). One row per period_type, period_start, measure and
-- source that has a denominator. Holds the small counts the marts hide: for building them only, not for BI.
--   numerator    proportion: units that meet the measure; mean: the total (days or hours); median: NULL
--   denominator  units counted (stays, or people for the diabetes measure: each person once per period, by their latest
--                result in it; in "all" a person linked across sources is one person)
--   statistic    mean or median; NULL for proportions
-- all_time runs over the whole calendar of dim_date (period_start its first day).
WITH calendar AS (
    SELECT MIN(calendar_date) AS first_day FROM {{ ref('dim_date') }}
),
dated AS (
    SELECT e.*, d.month_start_date, d.quarter_start_date, d.year_start_date
    FROM {{ ref('int_quality_events') }} e
    JOIN {{ ref('dim_date') }} d ON d.calendar_date = e.event_date
),
by_source AS (
    SELECT measure_id, source, patient_sk, unit_sk, event_sk, event_at, meets, amount,
           month_start_date, quarter_start_date, year_start_date
    FROM dated
    UNION ALL
    SELECT measure_id, 'all', patient_sk, unit_sk, event_sk, event_at, meets, amount,
           month_start_date, quarter_start_date, year_start_date
    FROM dated
),
by_period AS (
    SELECT 'month' AS period_type, month_start_date AS period_start, b.* FROM by_source b
    UNION ALL
    SELECT 'quarter', quarter_start_date, b.* FROM by_source b
    UNION ALL
    SELECT 'year', year_start_date, b.* FROM by_source b
    UNION ALL
    SELECT 'all_time', c.first_day, b.* FROM by_source b CROSS JOIN calendar c
),
-- one row per counted unit in each cell: the latest event (only the diabetes measure has several per unit)
units AS (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY period_type, period_start, measure_id, source, unit_sk
                              ORDER BY event_at DESC, event_sk DESC) AS rn
    FROM by_period p
)
SELECT
    u.period_type,
    u.period_start,
    u.measure_id,
    u.source,
    MIN(u.quarter_start_date)                                                           AS quarter_start_date,
    MIN(u.year_start_date)                                                              AS year_start_date,
    CASE m.kind
        WHEN 'proportion' THEN CAST(SUM(CASE WHEN u.meets THEN 1 ELSE 0 END) AS DOUBLE)
        -- summed as DECIMAL, so the total does not depend on row order
        WHEN 'mean' THEN CAST(SUM(CAST(u.amount AS DECIMAL(18, 2))) AS DOUBLE)
    END                                                                                 AS numerator,
    COUNT(*)                                                                            AS denominator,
    CASE m.kind
        WHEN 'mean' THEN CAST(SUM(CAST(u.amount AS DECIMAL(18, 2))) AS DOUBLE) / COUNT(*)
        WHEN 'median' THEN CAST(MEDIAN(u.amount) AS DOUBLE)
    END                                                                                 AS statistic
FROM units u
JOIN {{ ref('quality_measures') }} m ON m.measure_id = u.measure_id
WHERE u.rn = 1
GROUP BY u.period_type, u.period_start, u.measure_id, u.source, m.kind
