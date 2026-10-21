-- Dimension lab: one row per LOINC code of the numeric lab results (silver_observation, category laboratory), plus the
-- Unknown member for results without a LOINC code. Display and unit are the most frequent ones sent with the code
-- (ties: the first alphabetically). Key labs are the ones in seed key_labs that the Gold tables and the bed board use.
WITH results AS (
    SELECT loinc, description, unit
    FROM {{ ref('silver_observation') }}
    WHERE category = 'laboratory' AND value_num IS NOT NULL AND loinc IS NOT NULL
),
display AS (
    SELECT loinc, description,
           ROW_NUMBER() OVER (PARTITION BY loinc ORDER BY COUNT(*) DESC, description) AS rn
    FROM results
    GROUP BY loinc, description
),
units AS (
    SELECT loinc, unit,
           ROW_NUMBER() OVER (PARTITION BY loinc ORDER BY COUNT(*) DESC, unit) AS rn
    FROM results
    GROUP BY loinc, unit
)
SELECT
    {{ surrogate_key(['d.loinc']) }}                                     AS lab_sk,
    d.loinc,
    d.description                                                        AS display,
    u.unit,
    CASE WHEN k.loinc IS NOT NULL THEN TRUE ELSE FALSE END               AS is_key_lab,
    k.lab                                                                AS key_lab_name
FROM display d
JOIN units u ON u.loinc = d.loinc AND u.rn = 1
LEFT JOIN {{ ref('key_labs') }} k ON k.loinc = d.loinc
WHERE d.rn = 1
UNION ALL
SELECT {{ unknown_key() }}, NULL, 'No LOINC code', NULL, FALSE, NULL
