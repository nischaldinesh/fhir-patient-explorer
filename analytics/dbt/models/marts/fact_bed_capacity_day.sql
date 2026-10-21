-- Fact bed capacity day. Grain: one row per bed of the hospital per day, from the first day fact_bed_day has a row to
-- the live feed's data date: the beds that could have been occupied at the midnight that ends capacity_date. The
-- denominator of the semantic layer's bed occupancy rate (bed_days / capacity_bed_days), so both sides count the same
-- midnights and join dim_location the same way. Master data only (beds of the Riverside General reference data, demo):
-- no patient, so the keys are plain MD5. Capacity is the beds known now; the reference data has no history of beds.
WITH span AS (
    SELECT MIN(b.census_date) AS first_date, MAX(f.data_date) AS last_date
    FROM {{ ref('fact_bed_day') }} b
    CROSS JOIN {{ ref('gold_feed_freshness') }} f
),
beds AS (
    SELECT location_sk, location_id
    FROM {{ ref('dim_location') }}
    WHERE is_bed AND location_id IS NOT NULL
)
SELECT
    {{ surrogate_key(['b.location_id', 'CAST(d.calendar_date AS VARCHAR)']) }}       AS bed_capacity_day_sk,
    d.date_key                                                                       AS capacity_date_key,
    d.calendar_date                                                                  AS capacity_date,
    b.location_sk
FROM beds b
CROSS JOIN span s
JOIN {{ ref('dim_date') }} d
  ON d.calendar_date >= s.first_date
 AND d.calendar_date <= s.last_date
