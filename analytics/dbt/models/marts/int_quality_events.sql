-- Internal: the events the hospital quality measures count, one row per measure and counted unit (a stay, or for the
-- diabetes measure each HbA1c result of a person), with the day that puts it in a period. mart_quality_monthly and
-- mart_quality_period aggregate these, so every period and the pooled "all" source use the same definitions:
--   readmission_30d            index stay = hospital admission with a discharge; meets = readmitted within 30 days
--                              (gold_encounter_summary.readmitted_30d). Dated by discharge. All cause; planned
--                              readmissions and deaths in hospital are not excluded (unlike CMS HRRP).
--   los_mean_days, los_median_days  the same index stays; amount = length of stay in days. Dated by discharge.
--   ed_los_median_hours        emergency department visits that went home (kind emergency visit) with an end; amount =
--                              hours from arrival to departure (in the spirit of CMS OP 18). Dated by departure.
--   diabetes_a1c_under_8       HEDIS style, not certified: every numeric HbA1c result (LOINC 4548-4, 17856-6 or 4549-2,
--                              unit %, no comparator) of a person in the diabetes_type2 cohort; meets = under 8 percent.
--                              A period counts each person once, by their latest result in it. Dated by the result.
--   potassium_abnormal_on_admission  hospital admissions with a potassium result (key lab potassium) of the same record
--                              in the 24 hours from admission; meets = the first such result is under 3.5 or over
--                              5.0 mmol/L. Dated by admission.
WITH enc AS (
    SELECT * FROM {{ ref('fact_encounter') }}
),
index_stays AS (
    SELECT * FROM enc WHERE is_admission AND discharge_at IS NOT NULL AND length_of_stay_days IS NOT NULL
),
diabetic_people AS (
    SELECT DISTINCT k.patient_sk
    FROM {{ ref('gold_cohort_members') }} m
    JOIN {{ ref('int_patient_key') }} k ON k.patient_id = m.patient_id
    WHERE m.cohort = 'diabetes_type2'
),
potassium AS (
    SELECT
        e.encounter_sk, e.patient_sk, e.source, e.admit_at, r.value_num,
        ROW_NUMBER() OVER (PARTITION BY e.encounter_sk ORDER BY r.effective_at, r.lab_result_sk) AS rn
    FROM enc e
    JOIN {{ ref('fact_lab_result') }} r
      ON r.patient_record_sk = e.patient_record_sk
     AND r.effective_at >= e.admit_at
     AND DATEDIFF('minute', e.admit_at, r.effective_at) < 24 * 60
    JOIN {{ ref('key_labs') }} k ON k.loinc = r.loinc AND k.lab = 'potassium'
    WHERE e.is_admission AND r.comparator IS NULL
)
SELECT 'readmission_30d' AS measure_id, source, patient_sk, encounter_sk AS unit_sk, encounter_sk AS event_sk,
       CAST(discharge_at AS DATE) AS event_date, discharge_at AS event_at, readmitted_30d AS meets,
       CAST(NULL AS DOUBLE) AS amount
FROM index_stays
UNION ALL
SELECT 'los_mean_days', source, patient_sk, encounter_sk, encounter_sk,
       CAST(discharge_at AS DATE), discharge_at, CAST(NULL AS BOOLEAN), length_of_stay_days
FROM index_stays
UNION ALL
SELECT 'los_median_days', source, patient_sk, encounter_sk, encounter_sk,
       CAST(discharge_at AS DATE), discharge_at, CAST(NULL AS BOOLEAN), length_of_stay_days
FROM index_stays
UNION ALL
SELECT 'ed_los_median_hours', source, patient_sk, encounter_sk, encounter_sk,
       CAST(discharge_at AS DATE), discharge_at, CAST(NULL AS BOOLEAN), length_of_stay_hours
FROM enc
WHERE encounter_kind = 'emergency visit' AND discharge_at IS NOT NULL AND length_of_stay_hours IS NOT NULL
UNION ALL
SELECT 'diabetes_a1c_under_8', r.source, r.patient_sk, r.patient_sk, r.lab_result_sk,
       r.effective_date, r.effective_at, CASE WHEN r.value_num < 8.0 THEN TRUE ELSE FALSE END, r.value_num
FROM {{ ref('fact_lab_result') }} r
JOIN diabetic_people d ON d.patient_sk = r.patient_sk
WHERE r.loinc IN ('4548-4', '17856-6', '4549-2') AND r.comparator IS NULL AND r.unit = '%'
UNION ALL
SELECT 'potassium_abnormal_on_admission', source, patient_sk, encounter_sk, encounter_sk,
       CAST(admit_at AS DATE), admit_at, CASE WHEN value_num < 3.5 OR value_num > 5.0 THEN TRUE ELSE FALSE END, value_num
FROM potassium
WHERE rn = 1
