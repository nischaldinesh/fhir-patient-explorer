-- Silver patient: one row per patient, typed, deduplicated on resource id (newest load wins).
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Patient'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
),
typed AS (
    SELECT
        resource_id                                            AS patient_id,
        CASE
            WHEN 'hl7v2' IN ({{ json_str('raw', 'meta.tag[0].code') }}, {{ json_str('raw', 'meta.tag[1].code') }}, {{ json_str('raw', 'meta.tag[2].code') }}) THEN 'hl7v2'
            WHEN LOWER(COALESCE({{ json_str('raw', 'meta.profile[0]') }}, '')) LIKE '%mimic%'     THEN 'mimic'
            WHEN LOWER(COALESCE({{ json_str('raw', 'identifier[0].system') }}, '')) LIKE '%synthea%'
              OR LOWER(COALESCE({{ json_str('raw', 'identifier[0].system') }}, '')) LIKE '%synthetichealth%' THEN 'synthea'
            ELSE 'unknown'
        END                                                    AS source,
        {{ json_str('raw', 'identifier[0].value') }}                          AS identifier_0,
        {{ json_str('raw', 'identifier[1].value') }}                          AS identifier_1,
        {{ json_str('raw', 'name[0].family') }}                               AS family_raw,
        {{ json_str('raw', 'name[0].given[0]') }}                             AS given_raw,
        {{ json_str('raw', 'gender') }}                                       AS gender,
        TRY_CAST({{ json_str('raw', 'birthDate') }} AS DATE)                  AS birth_date,
        {{ json_ts('raw', 'deceasedDateTime') }}                           AS deceased_at,
        {{ json_str('raw', 'deceasedBoolean') }}                              AS deceased_flag,
        {{ json_str('raw', 'address[0].city') }}                              AS city,
        {{ json_str('raw', 'address[0].state') }}                             AS state,
        {{ json_str('raw', 'address[0].postalCode') }}                        AS postal_code,
        source_file,
        loaded_at
    FROM latest
)
SELECT
    patient_id,
    source,
    -- Synthea keeps the medical record number in its second identifier
    CASE WHEN source = 'synthea' THEN identifier_1 ELSE identifier_0 END        AS mrn,
    -- Synthea appends digits to names (Hickle134), so strip them there only.
    -- MIMIC names are Patient_10014729: the digits are the MIMIC subject id and must stay.
    CASE WHEN source = 'synthea' THEN REGEXP_REPLACE(family_raw, '[0-9]+$', '') ELSE REPLACE(family_raw, '_', ' ') END AS family_name,
    CASE WHEN source = 'synthea' THEN REGEXP_REPLACE(given_raw, '[0-9]+$', '') ELSE given_raw END                     AS given_name,
    gender,
    birth_date,
    deceased_at,
    CASE WHEN deceased_at IS NOT NULL OR deceased_flag = 'true' THEN TRUE ELSE FALSE END AS is_deceased,
    city,
    state,
    postal_code,
    source_file,
    loaded_at
FROM typed
