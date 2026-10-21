-- Gold acute kidney injury signal: serum creatinine (LOINC 2160-0) rising by 0.3 mg/dL or more within
-- 48 hours. This is one of the KDIGO criteria, simplified: it ignores urine output and the 1.5 times
-- baseline rule, so it is a screening signal for review, not a diagnosis. One row per patient, from the
-- result level rises in gold_aki_rise.
SELECT
    patient_id,
    COUNT(DISTINCT observation_id) AS n_signal_results,
    MIN(signal_at)                 AS first_signal_at,
    ROUND(MAX(rise), 2)            AS max_rise_mg_dl
FROM {{ ref('gold_aki_rise') }}
GROUP BY patient_id
