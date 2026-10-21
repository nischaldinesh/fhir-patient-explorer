-- Analyst view of the patient summary: pseudonym instead of the patient id, ages 90 and over withheld.
SELECT
    {{ patient_key('patient_id') }} AS patient_key,
    source, gender, is_deceased,
    CASE WHEN age_years >= 90 THEN NULL ELSE age_years END    AS age_years,
    n_encounters, n_admissions, inpatient_days, readmissions_30d,
    n_diagnoses, n_diagnoses_icd10, n_diagnoses_need_review,
    n_lab_results, n_lab_out_of_range, latest_bmi, bmi_category, has_aki_signal
FROM {{ ref('gold_patient_summary') }}
