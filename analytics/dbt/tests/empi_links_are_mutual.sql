-- The EMPI links both records of a pair: every Patient.link to a known patient has a link back.
SELECT l.patient_id, l.other_patient_id
FROM {{ ref('silver_patient_link') }} l
JOIN {{ ref('silver_patient') }} o ON o.patient_id = l.other_patient_id
LEFT JOIN {{ ref('silver_patient_link') }} back
       ON back.patient_id = l.other_patient_id AND back.other_patient_id = l.patient_id
WHERE back.patient_id IS NULL
