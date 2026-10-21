-- Live feed data quality: a visit of the live Riverside General feed is never discharged before it was admitted.
SELECT encounter_id, start_at, end_at
FROM {{ ref('gold_live_encounter') }}
WHERE end_at < start_at
