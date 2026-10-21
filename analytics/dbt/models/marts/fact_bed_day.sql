-- Fact bed day. Grain: one row per occupied bed per midnight: a visit was in a bed of the hospital (an Encounter.location
-- entry that is a bed with a Location reference) at the midnight that ends census_date. The entry's period is clipped to
-- the visit's own period, and an open entry or visit counts up to the live feed's data date. The same midnight rule as
-- gold_daily_census, so for the live feed's admissions the rows of a day add up to in_house_at_midnight (a test checks).
-- Only the Riverside General feed sends beds; MIMIC, Synthea and the original 25 patient feed have no rows here.
WITH fresh AS (
    SELECT data_date FROM {{ ref('gold_feed_freshness') }}
),
stays AS (
    SELECT
        el.encounter_id,
        el.location_id,
        GREATEST(el.start_at, e.start_at)                                    AS in_at,
        CASE WHEN el.end_at IS NULL THEN e.end_at
             WHEN e.end_at IS NULL THEN el.end_at
             ELSE LEAST(el.end_at, e.end_at) END                             AS out_at
    FROM {{ ref('silver_encounter_location') }} el
    JOIN {{ ref('silver_encounter') }} e ON e.encounter_id = el.encounter_id
    WHERE el.physical_type = 'bd' AND el.location_id IS NOT NULL
      AND el.start_at IS NOT NULL AND e.start_at IS NOT NULL
),
nights AS (
    SELECT s.encounter_id, s.location_id, d.calendar_date AS census_date
    FROM stays s
    CROSS JOIN fresh f
    JOIN {{ ref('dim_date') }} d
      ON d.calendar_date >= CAST(s.in_at AS DATE)
     AND d.calendar_date <= f.data_date
     AND s.in_at < CAST(d.calendar_date + 1 AS TIMESTAMP)
     AND (s.out_at IS NULL OR s.out_at >= CAST(d.calendar_date + 1 AS TIMESTAMP))
)
SELECT
    {{ salted_key(['n.encounter_id', 'n.location_id', 'n.census_date']) }}             AS bed_day_sk,
    {{ date_key('n.census_date') }}                                                    AS census_date_key,
    n.census_date,
    {{ salted_key(['n.encounter_id']) }}                                               AS encounter_sk,
    k.patient_sk,
    {{ surrogate_key(['n.location_id']) }}                                             AS location_sk,
    g.source,
    g.is_admission,
    CASE WHEN l.encounter_id IS NOT NULL THEN TRUE ELSE FALSE END                      AS is_live_feed
FROM nights n
JOIN {{ ref('gold_encounter_summary') }} g       ON g.encounter_id = n.encounter_id
LEFT JOIN {{ ref('int_patient_key') }} k         ON k.patient_id = g.patient_id
LEFT JOIN {{ ref('gold_live_encounter') }} l     ON l.encounter_id = n.encounter_id
