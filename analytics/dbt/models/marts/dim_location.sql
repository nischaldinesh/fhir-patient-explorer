-- Dimension location: one row per place (site, unit, room, bed, clinic, and the locations other sources send), plus
-- the Unknown member. Each place carries its unit, room and bed where they apply, and the bed capacity of its unit and
-- room (the beds of the Riverside General reference data, demo). bed_status is the status now, not historical.
WITH loc AS (
    SELECT * FROM {{ ref('silver_location') }}
),
beds AS (
    SELECT location_id, unit_location_id, part_of_location_id AS room_location_id
    FROM loc
    WHERE physical_type = 'bd'
),
unit_capacity AS (
    SELECT unit_location_id, COUNT(*) AS beds FROM beds GROUP BY unit_location_id
),
room_capacity AS (
    SELECT room_location_id, COUNT(*) AS beds FROM beds GROUP BY room_location_id
)
SELECT
    {{ surrogate_key(['l.location_id']) }}                                            AS location_sk,
    l.location_id,
    l.name                                                                            AS location_name,
    CASE l.physical_type
        WHEN 'si' THEN 'site' WHEN 'wa' THEN 'unit' WHEN 'ro' THEN 'room' WHEN 'bd' THEN 'bed'
        WHEN 'wi' THEN 'clinic' ELSE 'other'
    END                                                                               AS location_level,
    u.name                                                                            AS unit_name,
    CASE WHEN l.physical_type = 'ro' THEN l.name WHEN l.physical_type = 'bd' THEN r.name END AS room_name,
    CASE WHEN l.physical_type = 'bd' THEN l.name END                                  AS bed_name,
    CASE WHEN l.physical_type = 'bd' THEN TRUE ELSE FALSE END                         AS is_bed,
    CAST(COALESCE(uc.beds, 0) AS INTEGER)                                             AS unit_bed_capacity,
    CAST(CASE WHEN l.physical_type = 'ro' THEN COALESCE(rc.beds, 0)
              WHEN l.physical_type = 'bd' THEN COALESCE(rb.beds, 0) ELSE 0 END AS INTEGER) AS room_bed_capacity,
    CASE WHEN l.physical_type <> 'bd' THEN NULL
         WHEN l.operational_status = 'O' THEN 'occupied'
         WHEN l.operational_status = 'U' THEN 'unoccupied'
         ELSE 'unknown' END                                                           AS bed_status
FROM loc l
LEFT JOIN loc u            ON u.location_id = l.unit_location_id
LEFT JOIN loc r            ON r.location_id = l.part_of_location_id AND r.physical_type = 'ro'
LEFT JOIN unit_capacity uc ON uc.unit_location_id = l.unit_location_id
LEFT JOIN room_capacity rc ON rc.room_location_id = l.location_id
LEFT JOIN room_capacity rb ON rb.room_location_id = l.part_of_location_id
UNION ALL
SELECT {{ unknown_key() }}, NULL, 'Unknown', 'unknown', NULL, NULL, NULL, FALSE, 0, 0, NULL
