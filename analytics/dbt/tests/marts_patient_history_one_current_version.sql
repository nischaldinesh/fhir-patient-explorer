-- dim_patient_history: every record has exactly one current version, and versions of a record do not overlap.
WITH v AS (
    SELECT patient_record_sk, valid_from, valid_to, is_current,
           LEAD(valid_from) OVER (PARTITION BY patient_record_sk ORDER BY valid_from) AS next_from
    FROM {{ ref('dim_patient_history') }}
)
SELECT patient_record_sk, 'current versions' AS problem
FROM v GROUP BY patient_record_sk HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) > 1
UNION ALL
SELECT patient_record_sk, 'versions overlap'
FROM v WHERE next_from IS NOT NULL AND (valid_to IS NULL OR valid_to <> next_from)
