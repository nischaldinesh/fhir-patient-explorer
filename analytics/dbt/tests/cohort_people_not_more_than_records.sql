-- A cohort never holds more people than records: linked records of one person count once, never twice, and every
-- cohort with records has at least one person.
SELECT cohort, people, records
FROM {{ ref('gold_cohorts') }}
WHERE people > records OR records <> patients OR (records > 0 AND people < 1)
