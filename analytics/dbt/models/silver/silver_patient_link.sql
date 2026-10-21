-- Silver patient link: one row per Patient.link, the records the EMPI (empi/match.py) found to be the same person.
-- Links are written both ways, type seealso; records are never merged.
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Patient'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
)
SELECT
    l.resource_id                                                                 AS patient_id,
    {{ json_row_index('k') }}                                                     AS link_index,
    REPLACE({{ json_str('k.value', 'other.reference') }}, 'Patient/', '')         AS other_patient_id,
    {{ json_str('k.value', 'type') }}                                             AS link_type
FROM latest l {{ json_rows('l.raw', 'link', 'k') }}
