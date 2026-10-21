-- dim_patient has one member per person of gold_patient_crosswalk, and its record counts add up to the records.
WITH d AS (
    SELECT COUNT(*) AS people, SUM(record_count) AS records FROM {{ ref('dim_patient') }}
),
x AS (
    SELECT COUNT(DISTINCT master_id) AS people, COUNT(*) AS records FROM {{ ref('gold_patient_crosswalk') }}
)
SELECT d.people, x.people AS crosswalk_people, d.records, x.records AS crosswalk_records
FROM d CROSS JOIN x
WHERE d.people <> x.people OR COALESCE(d.records, 0) <> x.records
