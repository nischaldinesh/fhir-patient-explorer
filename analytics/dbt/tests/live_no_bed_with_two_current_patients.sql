-- Live feed data quality: no bed holds two patients right now (two visits in progress with an open entry for it).
SELECT bed_location_id, COUNT(*) AS current_patients
FROM {{ ref('gold_bed_board') }}
WHERE encounter_id IS NOT NULL
GROUP BY bed_location_id
HAVING COUNT(*) > 1
