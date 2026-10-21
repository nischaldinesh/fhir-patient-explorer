-- Gold patient summary: one row per patient, everything an analyst asks first.
-- Age: at death for deceased patients; hidden for living MIMIC patients (dates are shifted);
-- otherwise as of the extract date, so a rebuild of the same extract gives the same answer.
WITH as_of AS (
    SELECT CAST(MAX(loaded_at) AS DATE) AS as_of_date FROM {{ ref('bronze_resources') }}
),
enc AS (
    SELECT
        patient_id,
        COUNT(*)                                                             AS n_encounters,
        SUM(CASE WHEN is_admission THEN 1 ELSE 0 END)                        AS n_admissions,
        -- Summed as DECIMAL: every stay has 2 decimals, so the total is exact and does not depend on row order
        CAST(ROUND(SUM(CASE WHEN is_admission THEN COALESCE(CAST(length_of_stay_days AS DECIMAL(18, 2)), 0) ELSE 0 END), 1) AS DOUBLE) AS inpatient_days,
        SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END)                      AS readmissions_30d
    FROM {{ ref('gold_encounter_summary') }}
    GROUP BY patient_id
),
dx AS (
    SELECT
        patient_id,
        COUNT(*)                                                     AS n_diagnoses,
        SUM(CASE WHEN icd10_code IS NOT NULL THEN 1 ELSE 0 END)      AS n_diagnoses_icd10,
        SUM(CASE WHEN needs_review THEN 1 ELSE 0 END)                AS n_diagnoses_need_review
    FROM {{ ref('silver_condition') }}
    GROUP BY patient_id
),
labs AS (
    SELECT
        patient_id,
        COUNT(*)                                                         AS n_lab_results,
        SUM(CASE WHEN range_flag IN ('high', 'low') THEN 1 ELSE 0 END)  AS n_lab_out_of_range
    FROM {{ ref('silver_observation') }}
    WHERE category = 'laboratory'
    GROUP BY patient_id
)
SELECT
    p.patient_id,
    p.source,
    p.gender,
    p.is_deceased,
    CASE
        WHEN p.is_deceased AND p.deceased_at IS NOT NULL
            THEN CAST(FLOOR(DATEDIFF('day', p.birth_date, CAST(p.deceased_at AS DATE)) / 365.25) AS INTEGER)
        WHEN p.source = 'mimic' THEN NULL
        ELSE CAST(FLOOR(DATEDIFF('day', p.birth_date, a.as_of_date) / 365.25) AS INTEGER)
    END                                                   AS age_years,
    CASE
        WHEN p.is_deceased AND p.deceased_at IS NOT NULL THEN 'at death'
        WHEN p.source = 'mimic' THEN 'hidden: dates shifted'
        ELSE 'as of extract'
    END                                                   AS age_basis,
    COALESCE(e.n_encounters, 0)                           AS n_encounters,
    COALESCE(e.n_admissions, 0)                           AS n_admissions,
    COALESCE(e.inpatient_days, 0)                         AS inpatient_days,
    COALESCE(e.readmissions_30d, 0)                       AS readmissions_30d,
    COALESCE(d.n_diagnoses, 0)                            AS n_diagnoses,
    COALESCE(d.n_diagnoses_icd10, 0)                      AS n_diagnoses_icd10,
    COALESCE(d.n_diagnoses_need_review, 0)                AS n_diagnoses_need_review,
    COALESCE(l.n_lab_results, 0)                          AS n_lab_results,
    COALESCE(l.n_lab_out_of_range, 0)                     AS n_lab_out_of_range,
    b.bmi_computed                                        AS latest_bmi,
    b.bmi_category,
    CASE WHEN k.patient_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_aki_signal
FROM {{ ref('silver_patient') }} p
CROSS JOIN as_of a
LEFT JOIN enc e             ON e.patient_id = p.patient_id
LEFT JOIN dx d              ON d.patient_id = p.patient_id
LEFT JOIN labs l            ON l.patient_id = p.patient_id
LEFT JOIN {{ ref('gold_bmi') }} b        ON b.patient_id = p.patient_id
LEFT JOIN {{ ref('gold_aki_signal') }} k ON k.patient_id = p.patient_id
