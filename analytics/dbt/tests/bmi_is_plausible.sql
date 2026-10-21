-- BMI is computed only from plausible weights and heights, so it is always a positive number.
SELECT patient_id, weight_kg, height_cm, bmi_computed
FROM {{ ref('gold_bmi') }}
WHERE bmi_computed IS NULL OR bmi_computed <= 0
