-- Fact diagnosis. Grain: one row per encounter diagnosis, a Condition that points at a known encounter. Diagnoses
-- without an ICD 10 code point at the Unknown member of dim_diagnosis and keep the code system and code they came in.
SELECT
    {{ salted_key(['c.condition_id']) }}                                                 AS diagnosis_fact_sk,
    {{ salted_key(['c.encounter_id']) }}                                                 AS encounter_sk,
    k.patient_sk,
    CASE WHEN c.icd10_code IS NULL THEN {{ unknown_key() }}
         ELSE {{ surrogate_key(['c.icd10_code']) }} END                                  AS diagnosis_sk,
    {{ date_key('CAST(c.recorded_at AS DATE)') }}                                        AS recorded_date_key,
    k.source,
    c.code_system,
    c.code                                                                               AS source_code,
    c.icd10_code,
    c.icd10_origin,
    c.needs_review
FROM {{ ref('silver_condition') }} c
JOIN {{ ref('silver_encounter') }} e       ON e.encounter_id = c.encounter_id
LEFT JOIN {{ ref('int_patient_key') }} k   ON k.patient_id = c.patient_id
