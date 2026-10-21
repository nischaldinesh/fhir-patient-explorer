-- Live feed data quality: every lab result of the live feed has a LOINC code, or its name is on the reviewed list of
-- labs known to arrive without one (seed unmapped_labs).
SELECT o.observation_id, o.code, o.description
FROM {{ ref('silver_observation') }} o
JOIN {{ ref('gold_live_encounter') }} e ON e.encounter_id = o.encounter_id
WHERE o.category = 'laboratory'
  AND o.loinc IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref('unmapped_labs') }} u
      WHERE (u.match_type = 'code' AND UPPER(u.match) = UPPER(o.code))
         OR (u.match_type = 'text' AND u.match = LOWER(TRIM(o.description)))
  )
