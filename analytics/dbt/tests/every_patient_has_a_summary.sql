-- gold_patient_summary has exactly one row for every Silver patient.
SELECT p.patient_id
FROM {{ ref('silver_patient') }} p
LEFT JOIN {{ ref('gold_patient_summary') }} s ON s.patient_id = p.patient_id
WHERE s.patient_id IS NULL
UNION ALL
SELECT s.patient_id
FROM {{ ref('gold_patient_summary') }} s
LEFT JOIN {{ ref('silver_patient') }} p ON p.patient_id = s.patient_id
WHERE p.patient_id IS NULL
