-- The midnight census carries over: today's count is yesterday's plus today's admissions minus today's discharges.
WITH c AS (
    SELECT census_date, in_house_at_midnight, admissions, discharges,
           LAG(census_date) OVER (ORDER BY census_date)          AS prev_date,
           LAG(in_house_at_midnight) OVER (ORDER BY census_date) AS prev_in_house
    FROM {{ ref('gold_daily_census') }}
)
SELECT census_date, prev_in_house, admissions, discharges, in_house_at_midnight
FROM c
WHERE prev_date = census_date - 1
  AND in_house_at_midnight <> prev_in_house + admissions - discharges
