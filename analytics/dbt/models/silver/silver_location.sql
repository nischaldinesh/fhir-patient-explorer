-- Silver location: one row per place (site, unit, room, bed, clinic), deduplicated on resource id, with the unit
-- (physical type wa) each room or bed belongs to, found through Location.partOf.
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Location'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
),
typed AS (
    SELECT
        resource_id                                                                    AS location_id,
        {{ json_str('raw', 'name') }}                                                         AS name,
        {{ json_str('raw', 'status') }}                                                       AS status,
        {{ json_str('raw', 'physicalType.coding[0].code') }}                                  AS physical_type,
        REPLACE({{ json_str('raw', 'partOf.reference') }}, 'Location/', '')                   AS part_of_location_id,
        REPLACE({{ json_str('raw', 'managingOrganization.reference') }}, 'Organization/', '')  AS managing_organization_id,
        {{ json_str('raw', 'operationalStatus.code') }}                                       AS operational_status,
        source_file,
        loaded_at
    FROM latest
)
SELECT
    l.location_id,
    l.name,
    l.status,
    l.physical_type,
    l.part_of_location_id,
    CASE
        WHEN l.physical_type = 'wa' THEN l.location_id
        WHEN p.physical_type = 'wa' THEN p.location_id
        WHEN g.physical_type = 'wa' THEN g.location_id
    END                                                                                AS unit_location_id,
    l.managing_organization_id,
    l.operational_status,
    l.source_file,
    l.loaded_at
FROM typed l
LEFT JOIN typed p ON p.location_id = l.part_of_location_id
LEFT JOIN typed g ON g.location_id = p.part_of_location_id
