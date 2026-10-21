-- fact_bed_capacity_day holds every bed of dim_location on every day of its span, once: per day, as many rows as beds.
WITH beds AS (
    SELECT COUNT(*) AS n FROM {{ ref('dim_location') }} WHERE is_bed AND location_id IS NOT NULL
),
per_day AS (
    SELECT capacity_date, COUNT(*) AS n FROM {{ ref('fact_bed_capacity_day') }} GROUP BY capacity_date
)
SELECT p.capacity_date, p.n, b.n AS beds
FROM per_day p
CROSS JOIN beds b
WHERE p.n <> b.n
