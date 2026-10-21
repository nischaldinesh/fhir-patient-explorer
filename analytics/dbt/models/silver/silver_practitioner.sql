-- Silver practitioner: one row per member of staff, deduplicated on resource id.
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Practitioner'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
),
typed AS (
    SELECT
        resource_id                                                    AS practitioner_id,
        -- Synthea appends digits to names (Bergstrom287), as it does for patients
        REGEXP_REPLACE({{ json_str('raw', 'name[0].family') }}, '[0-9]+$', '')     AS family_name,
        REGEXP_REPLACE({{ json_str('raw', 'name[0].given[0]') }}, '[0-9]+$', '')   AS given_name,
        {{ json_str('raw', 'name[0].prefix[0]') }}                                 AS prefix,
        {{ json_str('raw', 'identifier[0].system') }}                              AS identifier_system,
        {{ json_str('raw', 'identifier[0].value') }}                               AS identifier_value,
        source_file,
        loaded_at
    FROM latest
)
SELECT
    practitioner_id,
    family_name,
    given_name,
    prefix,
    TRIM(COALESCE(prefix || ' ', '') || COALESCE(given_name || ' ', '') || COALESCE(family_name, ''))  AS display_name,
    identifier_system,
    identifier_value,
    source_file,
    loaded_at
FROM typed
