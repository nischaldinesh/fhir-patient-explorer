-- Silver condition: one row per diagnosis, with a standardized ICD 10 code where one is known.
--   * ICD 10 at the source (MIMIC ICD 10, HL7 v2 ICD 10, or an HL7 v2 ICD 9 already upgraded at ingest)
--   * MIMIC ICD 9 upgraded through the CMS GEM seed, only when CMS gives a single target
--   * otherwise NULL: SNOMED (Synthea) and ICD 9 codes that need a coder stay as they were
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Condition'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
),
typed AS (
    SELECT
        resource_id                                                AS condition_id,
        REPLACE({{ json_str('raw', 'subject.reference') }}, 'Patient/', '')       AS patient_id,
        REPLACE({{ json_str('raw', 'encounter.reference') }}, 'Encounter/', '')   AS encounter_id,
        {{ json_str('raw', 'code.coding[0].system') }}                            AS system_1,
        {{ json_str('raw', 'code.coding[0].code') }}                              AS code_1,
        {{ json_str('raw', 'code.coding[0].display') }}                           AS display_1,
        {{ json_str('raw', 'code.coding[1].system') }}                            AS system_2,
        {{ json_str('raw', 'code.coding[1].code') }}                              AS code_2,
        {{ json_str('raw', 'code.text') }}                                        AS code_text,
        {{ json_str('raw', 'clinicalStatus.coding[0].code') }}                    AS clinical_status,
        COALESCE({{ json_ts('raw', 'onsetDateTime') }}, {{ json_ts('raw', 'recordedDate') }}) AS recorded_at,
        -- FHIR servers may return meta.tag in any order (HAPI sorts them), so check every position used
        CASE WHEN 'needs-review' IN ({{ json_str('raw', 'meta.tag[0].code') }}, {{ json_str('raw', 'meta.tag[1].code') }}, {{ json_str('raw', 'meta.tag[2].code') }})
             THEN TRUE ELSE FALSE END AS needs_review,
        source_file,
        loaded_at
    FROM latest
)
SELECT
    t.condition_id,
    t.patient_id,
    t.encounter_id,
    CASE
        WHEN t.system_1 IN ('http://hl7.org/fhir/sid/icd-9-cm', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd9') THEN 'icd9'
        WHEN t.system_1 IN ('http://hl7.org/fhir/sid/icd-10-cm', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd10') THEN 'icd10'
        WHEN t.system_1 = 'http://snomed.info/sct' THEN 'snomed'
        WHEN t.system_1 IS NULL THEN 'none'
        ELSE 'other'
    END                                                            AS code_system,
    t.code_1                                                       AS code,
    COALESCE(t.code_text, t.display_1)                             AS description,
    UPPER(REPLACE(CASE
        WHEN t.system_1 IN ('http://hl7.org/fhir/sid/icd-10-cm', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd10') THEN t.code_1
        WHEN t.system_2 = 'http://hl7.org/fhir/sid/icd-10-cm' THEN t.code_2
        WHEN g.category IN ('exact', 'approximate') THEN g.icd10
    END, '.', ''))                                                 AS icd10_code,
    CASE
        WHEN t.system_1 IN ('http://hl7.org/fhir/sid/icd-10-cm', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd10') THEN 'source'
        WHEN t.system_2 = 'http://hl7.org/fhir/sid/icd-10-cm' THEN 'hl7v2_pipeline'
        WHEN g.category IN ('exact', 'approximate') THEN 'cms_gem_' || g.category
    END                                                            AS icd10_origin,
    CASE WHEN t.needs_review OR g.needs_review = 'yes' THEN TRUE ELSE FALSE END AS needs_review,
    t.clinical_status,
    t.recorded_at,
    t.source_file,
    t.loaded_at
FROM typed t
LEFT JOIN {{ ref('icd9_to_icd10') }} g
    ON t.system_1 = 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd9'
   AND g.icd9 = t.code_1
