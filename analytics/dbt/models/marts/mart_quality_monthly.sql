-- Mart quality monthly. Grain: one row per month, measure and source (mimic, synthea, hl7v2), for every month in which
-- the measure has a denominator. The measures and their exact definitions are in int_quality_events (and
-- analytics/README.md); the aggregates come from int_quality_cells, the same ones mart_quality_period uses.
-- Stays count visits, people count dim_patient members (one per master id). Small cell suppression (CMS cell size rule):
-- when the denominator, a count numerator, or the denominator minus that numerator is 1 to 10, the counts, rate and value
-- are NULL and is_suppressed is true. mart_quality_period checks that its quarters do not give these cells back.
WITH defined AS (
    SELECT
        c.*,
        m.measure_name,
        m.unit,
        m.counts,
        {{ is_small_cell('c.numerator', 'c.denominator', true) }}                   AS small_count_cell,
        {{ is_small_cell('c.numerator', 'c.denominator', false) }}                  AS small_denominator
    FROM {{ ref('int_quality_cells') }} c
    JOIN {{ ref('quality_measures') }} m ON m.measure_id = c.measure_id
    WHERE c.period_type = 'month' AND c.source <> 'all'
),
flagged AS (
    SELECT d.*,
           CASE WHEN d.unit = 'proportion' THEN d.small_count_cell ELSE d.small_denominator END AS small_cell
    FROM defined d
)
SELECT
    CAST(FLOOR({{ date_key('d.period_start') }} / 100) AS INTEGER)                   AS month_key,
    {{ date_key('d.period_start') }}                                                AS month_start_date_key,
    d.period_start                                                                  AS month_start_date,
    d.measure_id,
    d.measure_name,
    d.source,
    d.unit,
    d.counts,
    CASE WHEN d.small_cell THEN NULL ELSE d.numerator END                           AS numerator,
    CAST(CASE WHEN d.small_cell THEN NULL ELSE d.denominator END AS BIGINT)         AS denominator,
    CASE WHEN d.small_cell OR d.unit <> 'proportion' THEN NULL
         ELSE ROUND(d.numerator / d.denominator, 4) END                             AS rate,
    CASE WHEN d.small_cell THEN NULL
         WHEN d.unit = 'proportion' THEN ROUND(d.numerator / d.denominator, 4)
         ELSE ROUND(d.statistic, 2) END                                             AS value,
    d.small_cell                                                                    AS is_suppressed
FROM flagged d
