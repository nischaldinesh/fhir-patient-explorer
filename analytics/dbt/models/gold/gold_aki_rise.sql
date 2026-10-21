-- Gold kidney injury rises: one row per serum creatinine result (LOINC 2160-0) that is 0.3 mg/dL or more above a
-- result of the same patient from the 48 hours before it. gold_aki_signal sums these up per patient; the live feed
-- tables use the visit of the result (encounter_id) to flag a stay.
WITH creatinine AS (
    SELECT patient_id, encounter_id, observation_id, value_num, effective_at
    FROM {{ ref('silver_observation') }}
    WHERE loinc = '2160-0' AND value_num IS NOT NULL AND comparator IS NULL AND effective_at IS NOT NULL
)
SELECT
    later.patient_id,
    later.encounter_id,
    later.observation_id,
    later.effective_at                       AS signal_at,
    MAX(later.value_num - earlier.value_num) AS rise
FROM creatinine later
JOIN creatinine earlier
  ON earlier.patient_id = later.patient_id
 AND earlier.effective_at < later.effective_at
 AND DATEDIFF('minute', earlier.effective_at, later.effective_at) <= 48 * 60
WHERE later.value_num - earlier.value_num >= 0.3
GROUP BY later.patient_id, later.encounter_id, later.observation_id, later.effective_at
