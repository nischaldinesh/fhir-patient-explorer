-- Gold unit occupancy: per unit, beds occupied out of capacity, from the bed status on each Location, next to the
-- number of beds a live visit is in right now (gold_bed_board). The two agree when the feed keeps bed status current.
SELECT
    unit_location_id,
    unit_name,
    COUNT(*)                                                                    AS capacity,
    SUM(CASE WHEN is_occupied THEN 1 ELSE 0 END)                                AS occupied_beds,
    COUNT(encounter_id)                                                         AS beds_with_patient,
    ROUND(100.0 * SUM(CASE WHEN is_occupied THEN 1 ELSE 0 END) / COUNT(*), 1)   AS occupancy_pct
FROM {{ ref('gold_bed_board') }}
GROUP BY unit_location_id, unit_name
