-- Silver encounter: one row per visit, with its attending physician and where the patient was placed.
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Encounter'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
),
-- Encounter.participant of type ATND (attender) pointing at a Practitioner; the first one when there are several
attending AS (
    SELECT
        l.resource_id                                                                 AS encounter_id,
        REPLACE({{ json_str('p.value', 'individual.reference') }}, 'Practitioner/', '')       AS practitioner_id
    FROM latest l {{ json_rows('l.raw', 'participant', 'p') }}
    WHERE {{ json_str('p.value', 'type[0].coding[0].code') }} = 'ATND'
      AND {{ json_str('p.value', 'individual.reference') }} LIKE 'Practitioner/%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY l.resource_id ORDER BY {{ json_row_index('p') }}) = 1
),
-- The last place with a Location reference (the bed when beds are given), and how often the bed changed
places AS (
    SELECT
        encounter_id,
        location_id                                                                   AS last_location_id,
        GREATEST(COUNT(CASE WHEN physical_type = 'bd' THEN 1 END) OVER (PARTITION BY encounter_id) - 1, 0) AS bed_moves
    FROM {{ ref('silver_encounter_location') }}
    WHERE location_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY entry_index DESC) = 1
)
SELECT
    l.resource_id                                                  AS encounter_id,
    REPLACE({{ json_str('l.raw', 'subject.reference') }}, 'Patient/', '')           AS patient_id,
    {{ json_str('l.raw', 'status') }}                                               AS status,
    {{ json_str('l.raw', 'class.code') }}                                           AS class_code,
    REPLACE({{ json_str('l.raw', 'partOf.reference') }}, 'Encounter/', '')          AS part_of_encounter_id,
    {{ json_str('l.raw', 'identifier[0].system') }}                                 AS identifier_system,
    {{ json_str('l.raw', 'identifier[0].value') }}                                  AS identifier_value,
    {{ json_str('l.raw', 'priority.coding[0].code') }}                              AS priority_code,
    COALESCE({{ json_str('l.raw', 'type[0].text') }}, {{ json_str('l.raw', 'type[0].coding[0].display') }}) AS encounter_type,
    {{ json_ts('l.raw', 'period.start') }}                                       AS start_at,
    {{ json_ts('l.raw', 'period.end') }}                                         AS end_at,
    l.source_file,
    l.loaded_at,
    a.practitioner_id                                              AS attending_practitioner_id,
    pl.last_location_id,
    COALESCE(pl.bed_moves, 0)                                      AS bed_moves
FROM latest l
LEFT JOIN attending a ON a.encounter_id = l.resource_id
LEFT JOIN places pl ON pl.encounter_id = l.resource_id
