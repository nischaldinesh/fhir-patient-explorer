-- Gold terminology quality by source: how much of each source arrived in, or was mapped to, a standard code.
WITH labs AS (
    SELECT p.source,
           COUNT(*)                                            AS numeric_lab_results,
           SUM(CASE WHEN o.loinc IS NOT NULL THEN 1 ELSE 0 END) AS with_loinc
    FROM {{ ref('silver_observation') }} o
    JOIN {{ ref('silver_patient') }} p ON p.patient_id = o.patient_id
    WHERE o.category = 'laboratory' AND o.value_num IS NOT NULL
    GROUP BY p.source
),
dx AS (
    SELECT p.source,
           COUNT(*)                                                    AS diagnoses,
           SUM(CASE WHEN c.icd10_code IS NOT NULL THEN 1 ELSE 0 END)   AS with_icd10,
           SUM(CASE WHEN c.code_system = 'icd9' THEN 1 ELSE 0 END)     AS arrived_icd9,
           SUM(CASE WHEN c.code_system = 'snomed' THEN 1 ELSE 0 END)   AS arrived_snomed,
           SUM(CASE WHEN c.needs_review THEN 1 ELSE 0 END)             AS need_review
    FROM {{ ref('silver_condition') }} c
    JOIN {{ ref('silver_patient') }} p ON p.patient_id = c.patient_id
    GROUP BY p.source
)
SELECT
    s.source,
    COALESCE(l.numeric_lab_results, 0)                                              AS numeric_lab_results,
    COALESCE(l.with_loinc, 0)                                                       AS with_loinc,
    ROUND(100.0 * COALESCE(l.with_loinc, 0) / NULLIF(l.numeric_lab_results, 0), 1)  AS pct_loinc,
    COALESCE(d.diagnoses, 0)                                                        AS diagnoses,
    COALESCE(d.arrived_icd9, 0)                                                     AS arrived_icd9,
    COALESCE(d.arrived_snomed, 0)                                                   AS arrived_snomed,
    COALESCE(d.with_icd10, 0)                                                       AS with_icd10,
    ROUND(100.0 * COALESCE(d.with_icd10, 0) / NULLIF(d.diagnoses, 0), 1)            AS pct_icd10,
    COALESCE(d.need_review, 0)                                                      AS need_review
FROM (SELECT DISTINCT source FROM {{ ref('silver_patient') }}) s
LEFT JOIN labs l ON l.source = s.source
LEFT JOIN dx d   ON d.source = s.source
