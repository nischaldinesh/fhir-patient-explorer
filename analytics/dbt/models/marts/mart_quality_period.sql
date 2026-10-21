-- Mart quality period. Grain: one row per period_type (quarter, year, all_time), period_start, measure and source
-- (mimic, synthea, hl7v2, and all: the sources pooled), for every period in which the measure has a denominator. The
-- same measures as mart_quality_monthly (int_quality_events), over longer periods, so fewer cells are small.
--
-- Suppression. Totals give hidden cells back by subtraction (a quarter minus its shown months, "all" minus the shown
-- sources), so the small cell rule alone is not enough. Published cells are kept to a shape where subtraction can only
-- ever reach the residuals checked here:
--   small cell     the CMS cell size rule of mart_quality_monthly: the denominator, a count numerator, or the
--                  denominator minus that numerator is 1 to 10.
--   complementary  * by source, months (mart_quality_monthly), quarters, years and all_time nest like a tree. A cell is
--                    hidden when it minus the most a reader can cover with shown cells inside it (each shown child, and
--                    inside a hidden child its own cover) is 1 to 10 for any of the three counts.
--                  * "all" for a quarter or a year is shown only when every source with data in that period is shown,
--                    so it never tells more than the sources do (and the overlap of people linked across sources is not
--                    1 to 10). Otherwise an "all" year next to source totals would let a reader subtract across both.
--                  * "all" for all_time holds everything, so it is checked against the sources' all_time covers. When
--                    that residual is small, the shown source with the smallest denominator is hidden instead if that
--                    makes it safe (secondary suppression), so the pooled total stays visible.
-- Means and medians have no count numerator, so only their denominators are checked.
WITH cells AS (
    SELECT
        c.*,
        m.measure_name,
        m.unit,
        m.counts,
        CASE WHEN m.kind = 'proportion' THEN TRUE ELSE FALSE END                     AS is_count,
        COALESCE(c.numerator, 0)                                                     AS n,
        c.denominator                                                                AS d,
        c.denominator - COALESCE(c.numerator, 0)                                     AS c
    FROM {{ ref('int_quality_cells') }} c
    JOIN {{ ref('quality_measures') }} m ON m.measure_id = c.measure_id
),
-- months as mart_quality_monthly shows them (small cell rule only); a hidden month covers nothing
month_cover AS (
    SELECT measure_id, source, quarter_start_date,
           SUM(CASE WHEN shown THEN n ELSE 0 END) AS fn,
           SUM(CASE WHEN shown THEN d ELSE 0 END) AS fd,
           SUM(CASE WHEN shown THEN c ELSE 0 END) AS fc
    FROM (SELECT x.*, NOT {{ small_counts('x.n', 'x.d', 'x.c', 'x.is_count') }} AS shown
          FROM cells x WHERE x.period_type = 'month' AND x.source <> 'all') m
    GROUP BY measure_id, source, quarter_start_date
),
quarter_src AS (
    SELECT x.*,
           {{ small_counts('x.n', 'x.d', 'x.c', 'x.is_count') }}                     AS small_cell,
           {{ small_counts('x.n - COALESCE(k.fn, 0)', 'x.d - COALESCE(k.fd, 0)', 'x.c - COALESCE(k.fc, 0)', 'x.is_count') }} AS small_residual,
           COALESCE(k.fn, 0) AS kn, COALESCE(k.fd, 0) AS kd, COALESCE(k.fc, 0) AS kc
    FROM cells x
    LEFT JOIN month_cover k
      ON k.measure_id = x.measure_id AND k.source = x.source AND k.quarter_start_date = x.period_start
    WHERE x.period_type = 'quarter' AND x.source <> 'all'
),
quarter_src_cover AS (
    {{ cover_of('quarter_src') }}
),
year_src AS (
    SELECT x.*,
           {{ small_counts('x.n', 'x.d', 'x.c', 'x.is_count') }}                     AS small_cell,
           {{ small_counts('x.n - COALESCE(k.fn, 0)', 'x.d - COALESCE(k.fd, 0)', 'x.c - COALESCE(k.fc, 0)', 'x.is_count') }} AS small_residual,
           COALESCE(k.fn, 0) AS kn, COALESCE(k.fd, 0) AS kd, COALESCE(k.fc, 0) AS kc
    FROM cells x
    LEFT JOIN (SELECT measure_id, source, year_start_date, SUM(fn) AS fn, SUM(fd) AS fd, SUM(fc) AS fc
               FROM quarter_src_cover GROUP BY measure_id, source, year_start_date) k
      ON k.measure_id = x.measure_id AND k.source = x.source AND k.year_start_date = x.period_start
    WHERE x.period_type = 'year' AND x.source <> 'all'
),
year_src_cover AS (
    {{ cover_of('year_src') }}
),
all_time_src AS (
    SELECT x.*,
           {{ small_counts('x.n', 'x.d', 'x.c', 'x.is_count') }}                     AS small_cell,
           {{ small_counts('x.n - COALESCE(k.fn, 0)', 'x.d - COALESCE(k.fd, 0)', 'x.c - COALESCE(k.fc, 0)', 'x.is_count') }} AS small_residual,
           COALESCE(k.fn, 0) AS kn, COALESCE(k.fd, 0) AS kd, COALESCE(k.fc, 0) AS kc
    FROM cells x
    LEFT JOIN (SELECT measure_id, source, SUM(fn) AS fn, SUM(fd) AS fd, SUM(fc) AS fc
               FROM year_src_cover GROUP BY measure_id, source) k
      ON k.measure_id = x.measure_id AND k.source = x.source
    WHERE x.period_type = 'all_time' AND x.source <> 'all'
),
all_time_src_cover AS (
    {{ cover_of('all_time_src') }}
),
-- "all" for a quarter or a year: shown only when every source of the period is shown (it adds nothing to them)
period_all AS (
    SELECT x.*,
           {{ small_counts('x.n', 'x.d', 'x.c', 'x.is_count') }}                     AS small_cell,
           (COALESCE(s.hidden_sources, 0) > 0
            OR {{ small_counts('COALESCE(s.sn, 0) - x.n', 'COALESCE(s.sd, 0) - x.d', 'COALESCE(s.sc, 0) - x.c', 'x.is_count') }}) AS small_residual,
           FALSE AS secondary
    FROM cells x
    LEFT JOIN (SELECT period_type, measure_id, period_start, SUM(n) AS sn, SUM(d) AS sd, SUM(c) AS sc,
                      SUM(CASE WHEN small_cell OR small_residual THEN 1 ELSE 0 END) AS hidden_sources
               FROM (SELECT * FROM quarter_src_cover UNION ALL SELECT * FROM year_src_cover) u
               GROUP BY period_type, measure_id, period_start) s
      ON s.period_type = x.period_type AND s.measure_id = x.measure_id AND s.period_start = x.period_start
    WHERE x.period_type IN ('quarter', 'year') AND x.source = 'all'
),
-- "all" for all_time: the residual after the sources' covers (and the overlap of people linked across sources)
all_time_all AS (
    SELECT x.*,
           {{ small_counts('x.n', 'x.d', 'x.c', 'x.is_count') }}                     AS small_cell,
           {{ small_counts('COALESCE(s.sn, 0) - x.n', 'COALESCE(s.sd, 0) - x.d', 'COALESCE(s.sc, 0) - x.c', 'x.is_count') }} AS small_overlap,
           x.n - COALESCE(s.fn, 0) AS rn, x.d - COALESCE(s.fd, 0) AS rd, x.c - COALESCE(s.fc, 0) AS rc
    FROM cells x
    LEFT JOIN (SELECT measure_id, SUM(n) AS sn, SUM(d) AS sd, SUM(c) AS sc, SUM(fn) AS fn, SUM(fd) AS fd, SUM(fc) AS fc
               FROM all_time_src_cover GROUP BY measure_id) s
      ON s.measure_id = x.measure_id
    WHERE x.period_type = 'all_time' AND x.source = 'all'
),
all_time_secondary AS (
    SELECT a.measure_id, s.source,
           ROW_NUMBER() OVER (PARTITION BY a.measure_id ORDER BY s.d, s.source) AS pick
    FROM all_time_all a
    JOIN all_time_src_cover s ON s.measure_id = a.measure_id AND NOT (s.small_cell OR s.small_residual)
    WHERE NOT a.small_cell AND NOT a.small_overlap AND {{ small_counts('a.rn', 'a.rd', 'a.rc', 'a.is_count') }}
      AND NOT {{ small_counts('a.rn + s.n - s.kn', 'a.rd + s.d - s.kd', 'a.rc + s.c - s.kc', 'a.is_count') }}
),
all_time_all_final AS (
    SELECT a.*,
           a.small_overlap OR ({{ small_counts('a.rn', 'a.rd', 'a.rc', 'a.is_count') }} AND k.source IS NULL) AS small_residual,
           FALSE AS secondary
    FROM all_time_all a
    LEFT JOIN all_time_secondary k ON k.measure_id = a.measure_id AND k.pick = 1
),
by_source AS (
    SELECT q.*, FALSE AS secondary FROM quarter_src_cover q
    UNION ALL
    SELECT y.*, FALSE FROM year_src_cover y
    UNION ALL
    SELECT a.*, (k.source IS NOT NULL) FROM all_time_src_cover a
    LEFT JOIN all_time_secondary k ON k.measure_id = a.measure_id AND k.source = a.source AND k.pick = 1
),
decided AS (
    SELECT period_type, period_start, measure_id, measure_name, source, unit, counts, numerator, denominator, statistic,
           small_cell, small_residual OR secondary AS small_residual FROM by_source
    UNION ALL
    SELECT period_type, period_start, measure_id, measure_name, source, unit, counts, numerator, denominator, statistic,
           small_cell, small_residual FROM period_all
    UNION ALL
    SELECT period_type, period_start, measure_id, measure_name, source, unit, counts, numerator, denominator, statistic,
           small_cell, small_residual FROM all_time_all_final
),
calendar AS (
    SELECT MAX(calendar_date) AS last_day FROM {{ ref('dim_date') }}
),
ends AS (
    SELECT quarter_start_date AS period_start, 'quarter' AS period_type, MAX(calendar_date) AS period_end
    FROM {{ ref('dim_date') }} GROUP BY quarter_start_date
    UNION ALL
    SELECT year_start_date, 'year', MAX(calendar_date) FROM {{ ref('dim_date') }} GROUP BY year_start_date
)
SELECT
    d.period_type,
    d.period_start,
    COALESCE(e.period_end, c.last_day)                                              AS period_end,
    {{ date_key('d.period_start') }}                                                AS period_start_date_key,
    d.measure_id,
    d.measure_name,
    d.source,
    d.unit,
    d.counts,
    CASE WHEN d.small_cell OR d.small_residual THEN NULL ELSE d.numerator END       AS numerator,
    CAST(CASE WHEN d.small_cell OR d.small_residual THEN NULL ELSE d.denominator END AS BIGINT) AS denominator,
    CASE WHEN d.small_cell OR d.small_residual OR d.unit <> 'proportion' THEN NULL
         ELSE ROUND(d.numerator / d.denominator, 4) END                             AS rate,
    CASE WHEN d.small_cell OR d.small_residual THEN NULL
         WHEN d.unit = 'proportion' THEN ROUND(d.numerator / d.denominator, 4)
         ELSE ROUND(d.statistic, 2) END                                             AS value,
    CASE WHEN d.small_cell OR d.small_residual THEN TRUE ELSE FALSE END             AS is_suppressed,
    CASE WHEN d.small_cell THEN 'small cell'
         WHEN d.small_residual THEN 'complementary' END                             AS suppression_reason
FROM decided d
CROSS JOIN calendar c
LEFT JOIN ends e ON e.period_type = d.period_type AND e.period_start = d.period_start
