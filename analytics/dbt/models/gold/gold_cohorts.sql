-- Gold cohort sizes by source, from gold_cohort_members. patients and records count patient records; people counts
-- persons, one per master id of gold_patient_crosswalk, so a person the EMPI linked across sources counts once. The
-- per source columns count records, since one person can have a record in several sources.
WITH totals AS (
    SELECT COUNT(*) AS records, COUNT(DISTINCT master_id) AS people
    FROM {{ ref('gold_patient_crosswalk') }}
)
SELECT
    m.cohort,
    COUNT(*)                                                    AS patients,
    SUM(CASE WHEN p.source = 'mimic' THEN 1 ELSE 0 END)         AS mimic,
    SUM(CASE WHEN p.source = 'synthea' THEN 1 ELSE 0 END)       AS synthea,
    SUM(CASE WHEN p.source = 'hl7v2' THEN 1 ELSE 0 END)         AS hl7v2,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM {{ ref('silver_patient') }}), 1) AS pct_of_patients,
    COUNT(DISTINCT COALESCE(x.master_id, m.patient_id))         AS people,
    COUNT(*)                                                    AS records,
    ROUND(100.0 * COUNT(DISTINCT COALESCE(x.master_id, m.patient_id)) / (SELECT MAX(people) FROM totals), 1) AS pct_of_people
FROM {{ ref('gold_cohort_members') }} m
JOIN {{ ref('silver_patient') }} p ON p.patient_id = m.patient_id
LEFT JOIN {{ ref('gold_patient_crosswalk') }} x ON x.patient_id = m.patient_id
GROUP BY m.cohort
