-- Silver observation: one row per result, with a LOINC code where one is known
-- (at the source, or through the curated MIMIC lab seed).
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Observation'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
),
typed AS (
    SELECT
        resource_id                                                AS observation_id,
        REPLACE({{ json_str('raw', 'subject.reference') }}, 'Patient/', '')       AS patient_id,
        REPLACE({{ json_str('raw', 'encounter.reference') }}, 'Encounter/', '')   AS encounter_id,
        {{ json_str('raw', 'category[0].coding[0].code') }}                       AS category,
        {{ json_str('raw', 'status') }}                                           AS status,
        {{ json_str('raw', 'code.coding[0].system') }}                            AS code_system,
        {{ json_str('raw', 'code.coding[0].code') }}                              AS code,
        COALESCE({{ json_str('raw', 'code.text') }}, {{ json_str('raw', 'code.coding[0].display') }}) AS description,
        {{ json_num('raw', 'valueQuantity.value') }}                           AS value_num,
        {{ json_str('raw', 'valueQuantity.comparator') }}                         AS comparator,
        COALESCE({{ json_str('raw', 'valueQuantity.unit') }}, {{ json_str('raw', 'valueQuantity.code') }}) AS unit,
        {{ json_str('raw', 'valueString') }}                                      AS value_text,
        {{ json_num('raw', 'referenceRange[0].low.value') }}                   AS ref_low,
        {{ json_num('raw', 'referenceRange[0].high.value') }}                  AS ref_high,
        {{ json_str('raw', 'interpretation[0].coding[0].code') }}                 AS interpretation,
        COALESCE({{ json_ts('raw', 'effectiveDateTime') }}, {{ json_ts('raw', 'effectivePeriod.start') }}, {{ json_ts('raw', 'issued') }}) AS effective_at,
        source_file,
        loaded_at
    FROM latest
)
SELECT
    t.observation_id,
    t.patient_id,
    t.encounter_id,
    t.category,
    t.status,
    t.code_system,
    t.code,
    t.description,
    CASE
        WHEN t.code_system = 'http://loinc.org' THEN t.code
        WHEN t.code_system = 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-d-labitems' THEN m.loinc
    END                                                            AS loinc,
    t.value_num,
    t.comparator,
    t.unit,
    t.value_text,
    t.ref_low,
    t.ref_high,
    CASE
        WHEN t.value_num IS NULL OR t.comparator IS NOT NULL THEN NULL
        WHEN t.ref_high IS NOT NULL AND t.value_num > t.ref_high THEN 'high'
        WHEN t.ref_low IS NOT NULL AND t.value_num < t.ref_low THEN 'low'
        WHEN t.ref_low IS NOT NULL OR t.ref_high IS NOT NULL THEN 'normal'
    END                                                            AS range_flag,
    t.interpretation,
    t.effective_at,
    t.source_file,
    t.loaded_at
FROM typed t
LEFT JOIN {{ ref('lab_loinc_map') }} m
    ON t.code_system = 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-d-labitems'
   AND m.mimic_code = t.code
