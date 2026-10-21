-- Gold weekly metrics: the live Riverside General feed per ISO week (Monday to Sunday), the numbers a monthly quality
-- committee reviews. Counts:
--   admissions, ed_visits                 visits that started in the week
--   discharges                            inpatient discharges in the week (the index discharges)
--   readmissions_30d, readmission_rate_pct  index discharges in the week followed by another admission within 30 days,
--                                         out of the week's discharges. readmission_window_complete is false while
--                                         30 days have not passed since the end of the week, so the rate can still rise.
--   median_los_days, p90_los_days         length of stay of the week's discharges
--   abnormal_lab_rate_pct                 numeric lab results with a reference range flagged high or low
--   loinc_coverage_pct                    numeric lab results with a LOINC code
--   icd10_coverage_pct, needs_review      diagnoses of the visits that started in the week
--   kidney_signals                        visits whose first kidney injury signal fell in the week
-- is_complete_week is false for a week the feed has not fully covered yet (the current one, or the first one).
WITH live AS (
    SELECT * FROM {{ ref('gold_live_encounter') }}
),
fresh AS (
    SELECT data_date FROM {{ ref('gold_feed_freshness') }}
),
weeks AS (
    SELECT {{ week_start('census_date') }} AS week_start, COUNT(*) AS days_with_data
    FROM {{ ref('gold_daily_census') }}
    GROUP BY 1
),
started AS (
    SELECT {{ week_start('start_date') }}                    AS week_start,
           SUM(CASE WHEN is_admission THEN 1 ELSE 0 END)     AS admissions,
           SUM(CASE WHEN NOT is_admission THEN 1 ELSE 0 END) AS ed_visits
    FROM live
    GROUP BY 1
),
discharged AS (
    SELECT {{ week_start('end_date') }}                                            AS week_start,
           COUNT(*)                                                                AS discharges,
           SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END)                         AS readmissions_30d,
           ROUND(MEDIAN(length_of_stay_days), 2)                                   AS median_los_days,
           ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY length_of_stay_days), 2) AS p90_los_days
    FROM live
    WHERE is_admission AND end_date IS NOT NULL
    GROUP BY 1
),
labs AS (
    SELECT {{ week_start('CAST(o.effective_at AS DATE)') }}                         AS week_start,
           COUNT(*)                                                                 AS lab_results,
           SUM(CASE WHEN o.loinc IS NOT NULL THEN 1 ELSE 0 END)                     AS with_loinc,
           SUM(CASE WHEN o.range_flag IS NOT NULL THEN 1 ELSE 0 END)                AS flagged,
           SUM(CASE WHEN o.range_flag IN ('high', 'low') THEN 1 ELSE 0 END)         AS abnormal
    FROM {{ ref('silver_observation') }} o
    JOIN live ON live.encounter_id = o.encounter_id
    WHERE o.category = 'laboratory' AND o.value_num IS NOT NULL AND o.effective_at IS NOT NULL
    GROUP BY 1
),
dx AS (
    SELECT {{ week_start('live.start_date') }}                                       AS week_start,
           COUNT(*)                                                                 AS diagnoses,
           SUM(CASE WHEN c.icd10_code IS NOT NULL THEN 1 ELSE 0 END)                AS with_icd10,
           SUM(CASE WHEN c.needs_review THEN 1 ELSE 0 END)                          AS needs_review
    FROM {{ ref('silver_condition') }} c
    JOIN live ON live.encounter_id = c.encounter_id
    GROUP BY 1
),
kidney AS (
    SELECT {{ week_start('CAST(first_kidney_signal_at AS DATE)') }} AS week_start, COUNT(*) AS kidney_signals
    FROM live
    WHERE has_kidney_signal
    GROUP BY 1
)
SELECT
    w.week_start,
    w.week_start + 6                                                                  AS week_end,
    w.days_with_data,
    CASE WHEN w.days_with_data = 7 THEN TRUE ELSE FALSE END                           AS is_complete_week,
    COALESCE(s.admissions, 0)                                                         AS admissions,
    COALESCE(d.discharges, 0)                                                         AS discharges,
    COALESCE(s.ed_visits, 0)                                                          AS ed_visits,
    COALESCE(d.readmissions_30d, 0)                                                   AS readmissions_30d,
    ROUND(100.0 * d.readmissions_30d / NULLIF(d.discharges, 0), 1)                    AS readmission_rate_pct,
    CASE WHEN w.week_start + 36 <= f.data_date THEN TRUE ELSE FALSE END               AS readmission_window_complete,
    d.median_los_days,
    d.p90_los_days,
    COALESCE(l.lab_results, 0)                                                        AS lab_results,
    ROUND(100.0 * l.abnormal / NULLIF(l.flagged, 0), 1)                               AS abnormal_lab_rate_pct,
    ROUND(100.0 * l.with_loinc / NULLIF(l.lab_results, 0), 1)                         AS loinc_coverage_pct,
    COALESCE(x.diagnoses, 0)                                                          AS diagnoses,
    ROUND(100.0 * x.with_icd10 / NULLIF(x.diagnoses, 0), 1)                           AS icd10_coverage_pct,
    COALESCE(x.needs_review, 0)                                                       AS needs_review,
    COALESCE(k.kidney_signals, 0)                                                     AS kidney_signals
FROM weeks w
CROSS JOIN fresh f
LEFT JOIN started s    ON s.week_start = w.week_start
LEFT JOIN discharged d ON d.week_start = w.week_start
LEFT JOIN labs l       ON l.week_start = w.week_start
LEFT JOIN dx x         ON x.week_start = w.week_start
LEFT JOIN kidney k     ON k.week_start = w.week_start
