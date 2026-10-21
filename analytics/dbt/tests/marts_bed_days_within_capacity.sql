-- Bed occupancy can never pass 100 percent: every occupied bed of fact_bed_day is a bed of fact_bed_capacity_day on
-- the same day, so per unit and day the bed days never exceed the beds available.
SELECT b.census_date, b.location_sk
FROM {{ ref('fact_bed_day') }} b
LEFT JOIN {{ ref('fact_bed_capacity_day') }} c
  ON c.location_sk = b.location_sk AND c.capacity_date = b.census_date
WHERE c.bed_capacity_day_sk IS NULL
