-- fact_encounter has every visit of gold_encounter_summary once, with the same admissions and readmissions.
WITH f AS (
    SELECT COUNT(*) AS visits, SUM(CASE WHEN is_admission THEN 1 ELSE 0 END) AS admissions,
           SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END) AS readmissions
    FROM {{ ref('fact_encounter') }}
),
g AS (
    SELECT COUNT(*) AS visits, SUM(CASE WHEN is_admission THEN 1 ELSE 0 END) AS admissions,
           SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END) AS readmissions
    FROM {{ ref('gold_encounter_summary') }}
)
SELECT f.*, g.visits AS gold_visits
FROM f CROSS JOIN g
WHERE f.visits <> g.visits OR COALESCE(f.admissions, 0) <> COALESCE(g.admissions, 0)
   OR COALESCE(f.readmissions, 0) <> COALESCE(g.readmissions, 0)
