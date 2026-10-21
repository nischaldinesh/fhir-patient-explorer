-- The kidney injury signal list for analysts: who is flagged, by pseudonym only.
SELECT
    {{ patient_key('k.patient_id') }} AS patient_key,
    p.source, k.first_signal_at, k.max_rise_mg_dl, k.n_signal_results
FROM {{ ref('gold_aki_signal') }} k
JOIN {{ ref('silver_patient') }} p ON p.patient_id = k.patient_id
