-- Linked records are one person: both ends of every Patient.link carry the same master id, and every patient
-- record appears in the crosswalk exactly once.
SELECT l.patient_id
FROM {{ ref('silver_patient_link') }} l
JOIN {{ ref('gold_patient_crosswalk') }} a ON a.patient_id = l.patient_id
JOIN {{ ref('gold_patient_crosswalk') }} b ON b.patient_id = l.other_patient_id
WHERE a.master_id <> b.master_id
UNION ALL
SELECT patient_id
FROM {{ ref('silver_patient') }}
WHERE patient_id NOT IN (SELECT patient_id FROM {{ ref('gold_patient_crosswalk') }})
