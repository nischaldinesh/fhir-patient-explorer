-- Analyst view of cohort membership, keyed by the same pseudonym as the other analyst views.
SELECT cohort, {{ patient_key('patient_id') }} AS patient_key
FROM {{ ref('gold_cohort_members') }}
