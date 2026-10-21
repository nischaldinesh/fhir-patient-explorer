-- Live feed data quality: no admission is still open 30 days after it started (a lost discharge message).
SELECT e.encounter_id, e.start_at
FROM {{ ref('gold_live_encounter') }} e
CROSS JOIN {{ ref('gold_feed_freshness') }} f
WHERE e.is_admission AND e.end_at IS NULL
  AND DATEDIFF('day', e.start_date, f.data_date) > 30
