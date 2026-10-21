-- Gold BMI: weight (LOINC 29463-7, kg) / height (8302-2, cm, in metres) squared, using each patient's latest
-- weight and the height measured closest in time to it. Where a BMI was also reported (39156-5) it is kept
-- next to the computed one as a cross check. Adult categories only apply from age 20; for children BMI is
-- read against growth percentiles, so they are labelled instead of being forced into an adult category.
WITH weight AS (
    SELECT patient_id, value_num AS weight_kg, effective_at,
           ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY effective_at DESC, observation_id DESC) AS rn
    FROM {{ ref('silver_observation') }}
    WHERE loinc = '29463-7' AND unit = 'kg' AND value_num BETWEEN 1 AND 400 AND effective_at IS NOT NULL
),
latest_weight AS (SELECT * FROM weight WHERE rn = 1),
height AS (
    SELECT h.patient_id, h.value_num AS height_cm, h.effective_at AS height_at,
           ROW_NUMBER() OVER (PARTITION BY h.patient_id
                              ORDER BY ABS(DATEDIFF('day', h.effective_at, w.effective_at)), h.effective_at DESC) AS rn
    FROM {{ ref('silver_observation') }} h
    JOIN latest_weight w ON w.patient_id = h.patient_id
    WHERE h.loinc = '8302-2' AND h.unit = 'cm' AND h.value_num BETWEEN 30 AND 250 AND h.effective_at IS NOT NULL
),
reported AS (
    SELECT patient_id, value_num AS bmi_reported,
           ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY effective_at DESC, observation_id DESC) AS rn
    FROM {{ ref('silver_observation') }}
    WHERE loinc = '39156-5' AND value_num IS NOT NULL
),
joined AS (
    SELECT
        w.patient_id,
        p.source,
        w.weight_kg,
        h.height_cm,
        w.effective_at AS measured_at,
        ABS(DATEDIFF('day', h.height_at, w.effective_at))                        AS days_between_height_and_weight,
        ROUND(w.weight_kg / ((h.height_cm / 100.0) * (h.height_cm / 100.0)), 1)  AS bmi_computed,
        r.bmi_reported,
        CAST(FLOOR(DATEDIFF('day', p.birth_date, CAST(w.effective_at AS DATE)) / 365.25) AS INTEGER) AS age_at_measurement
    FROM latest_weight w
    JOIN height h ON h.patient_id = w.patient_id AND h.rn = 1
    JOIN {{ ref('silver_patient') }} p ON p.patient_id = w.patient_id
    LEFT JOIN reported r ON r.patient_id = w.patient_id AND r.rn = 1
)
SELECT
    *,
    CASE
        WHEN age_at_measurement < 20 THEN 'under 20: use growth percentiles'
        WHEN bmi_computed < 18.5 THEN 'underweight'
        WHEN bmi_computed < 25 THEN 'healthy weight'
        WHEN bmi_computed < 30 THEN 'overweight'
        ELSE 'obesity'
    END AS bmi_category
FROM joined
