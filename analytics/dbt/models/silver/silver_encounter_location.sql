-- Silver encounter location: one row per Encounter.location entry, so a visit can be followed from unit to unit
-- and bed to bed. A transfer completes the entries of the old place and adds entries for the new one.
-- An entry with only a display text (no Location reference) keeps location_id NULL.
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Encounter'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
)
SELECT
    l.resource_id                                                                    AS encounter_id,
    {{ json_row_index('loc') }}                                                      AS entry_index,
    REPLACE({{ json_str('loc.value', 'location.reference') }}, 'Location/', '')              AS location_id,
    {{ json_str('loc.value', 'location.display') }}                                          AS location_display,
    {{ json_str('loc.value', 'status') }}                                                    AS status,
    {{ json_str('loc.value', 'physicalType.coding[0].code') }}                               AS physical_type,
    {{ json_ts('loc.value', 'period.start') }}                                            AS start_at,
    {{ json_ts('loc.value', 'period.end') }}                                              AS end_at
FROM latest l {{ json_rows('l.raw', 'location', 'loc') }}
