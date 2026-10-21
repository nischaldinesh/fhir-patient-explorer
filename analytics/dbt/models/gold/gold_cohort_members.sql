-- Gold cohorts. Diagnosis cohorts come from seed_cohort_codes, which lists codes in ICD 10, ICD 9 and
-- SNOMED CT, so one definition finds patients from all three sources whatever code system they arrived in.
-- Lab cohorts and the readmission cohort are computed from the other Gold tables.
SELECT DISTINCT s.cohort, c.patient_id
FROM {{ ref('silver_condition') }} c
JOIN {{ ref('cohort_codes') }} s
  ON (s.code_system = 'icd10' AND c.icd10_code LIKE s.code_pattern)
  OR (s.code_system <> 'icd10' AND s.code_system = c.code_system AND UPPER(REPLACE(c.code, '.', '')) LIKE s.code_pattern)
UNION
SELECT 'low_sodium', o.patient_id
FROM {{ ref('silver_observation') }} o
JOIN {{ ref('key_labs') }} k ON k.loinc = o.loinc
WHERE k.lab = 'sodium' AND o.value_num < 135 AND o.comparator IS NULL
UNION
SELECT 'aki_signal', patient_id FROM {{ ref('gold_aki_signal') }}
UNION
SELECT 'readmitted_30d', patient_id FROM {{ ref('gold_encounter_summary') }} WHERE readmitted_30d
