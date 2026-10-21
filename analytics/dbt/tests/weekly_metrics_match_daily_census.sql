-- Every week's admissions, discharges and ED visits are the sum of its days in the daily census.
WITH daily AS (
    SELECT {{ week_start('census_date') }} AS week_start,
           SUM(admissions) AS admissions, SUM(discharges) AS discharges, SUM(ed_visits) AS ed_visits
    FROM {{ ref('gold_daily_census') }}
    GROUP BY 1
)
SELECT w.week_start
FROM {{ ref('gold_weekly_metrics') }} w
LEFT JOIN daily d ON d.week_start = w.week_start
WHERE d.week_start IS NULL
   OR w.admissions <> d.admissions OR w.discharges <> d.discharges OR w.ed_visits <> d.ed_visits
