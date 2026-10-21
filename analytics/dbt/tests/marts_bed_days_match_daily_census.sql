-- On every day of gold_daily_census, the occupied beds of fact_bed_day for the live feed's admissions add up to
-- in_house_at_midnight. An admission the feed never gave a bed of the hospital (a place sent as text only) is in the
-- census but has no bed days, so it is counted next to them with the same midnight rule.
WITH beds AS (
    SELECT census_date, COUNT(*) AS occupied
    FROM {{ ref('fact_bed_day') }}
    WHERE is_live_feed AND is_admission
    GROUP BY census_date
),
never_bedded AS (
    SELECT e.encounter_id, e.start_at, e.end_at
    FROM {{ ref('gold_live_encounter') }} e
    WHERE e.is_admission
      AND NOT EXISTS (SELECT 1 FROM {{ ref('silver_encounter_location') }} el
                      WHERE el.encounter_id = e.encounter_id AND el.physical_type = 'bd' AND el.location_id IS NOT NULL)
),
unbedded AS (
    SELECT c.census_date, COUNT(n.encounter_id) AS in_house
    FROM {{ ref('gold_daily_census') }} c
    JOIN never_bedded n
      ON n.start_at < CAST(c.census_date + 1 AS TIMESTAMP)
     AND (n.end_at IS NULL OR n.end_at >= CAST(c.census_date + 1 AS TIMESTAMP))
    GROUP BY c.census_date
)
SELECT c.census_date, c.in_house_at_midnight, b.occupied, u.in_house AS without_a_bed
FROM {{ ref('gold_daily_census') }} c
LEFT JOIN beds b     ON b.census_date = c.census_date
LEFT JOIN unbedded u ON u.census_date = c.census_date
WHERE c.in_house_at_midnight <> COALESCE(b.occupied, 0) + COALESCE(u.in_house, 0)
