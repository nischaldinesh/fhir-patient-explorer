-- The rule that keeps the pooled rows safe: an "all" cell for a quarter or a year is shown only when every source with
-- data in that period is shown too, so it can be added up from cells already shown and never tells more.
SELECT a.period_type, a.period_start, a.measure_id, s.source AS hidden_source
FROM {{ ref('mart_quality_period') }} a
JOIN {{ ref('mart_quality_period') }} s
  ON s.period_type = a.period_type AND s.period_start = a.period_start AND s.measure_id = a.measure_id
 AND s.source <> 'all'
WHERE a.source = 'all' AND a.period_type IN ('quarter', 'year')
  AND NOT a.is_suppressed AND s.is_suppressed
