-- Data quality: a bed never holds two visits at the same time (bed entries of Encounter.location).
SELECT a.location_id, a.encounter_id, b.encounter_id AS other_encounter_id, a.start_at, b.start_at AS other_start_at
FROM {{ ref('silver_encounter_location') }} a
JOIN {{ ref('silver_encounter_location') }} b
  ON b.location_id = a.location_id AND b.encounter_id > a.encounter_id
WHERE a.physical_type = 'bd' AND b.physical_type = 'bd'
  AND a.start_at IS NOT NULL AND b.start_at IS NOT NULL
  AND a.start_at < COALESCE(b.end_at, CAST('9999-12-31 00:00:00' AS TIMESTAMP))
  AND b.start_at < COALESCE(a.end_at, CAST('9999-12-31 00:00:00' AS TIMESTAMP))
