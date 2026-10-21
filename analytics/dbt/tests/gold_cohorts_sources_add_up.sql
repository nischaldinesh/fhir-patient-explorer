-- Every cohort member comes from exactly one source, so the per source counts add up to the total.
SELECT cohort, patients, mimic, synthea, hl7v2
FROM {{ ref('gold_cohorts') }}
WHERE patients <> mimic + synthea + hl7v2
