-- Dimension patient: one row per person, the master id of gold_patient_crosswalk, so records the EMPI linked across
-- sources are one member. No name, MRN, address below the state, exact birth date or FHIR id: the key is salted.
-- Attributes come from the group's representative record (master_patient_id, its smallest patient id), falling back
-- to the other records when it has none; the deceased flag is true when any record says so. Age and the 90 and over
-- rule follow the analyst views (HIPAA Safe Harbor): the age band of gold_patient_summary's age, no birth year from 90.
WITH records AS (
    SELECT
        k.patient_sk,
        k.patient_id,
        x.master_patient_id,
        p.source,
        p.gender,
        p.birth_date,
        p.is_deceased,
        p.state,
        s.age_years,
        CASE WHEN p.patient_id = x.master_patient_id THEN 0 ELSE 1 END AS preference
    FROM {{ ref('int_patient_key') }} k
    JOIN {{ ref('gold_patient_crosswalk') }} x ON x.patient_id = k.patient_id
    JOIN {{ ref('silver_patient') }} p         ON p.patient_id = k.patient_id
    LEFT JOIN {{ ref('gold_patient_summary') }} s ON s.patient_id = k.patient_id
),
ranked AS (
    SELECT r.*,
           ROW_NUMBER() OVER (PARTITION BY patient_sk ORDER BY preference, patient_id) AS rn
    FROM records r
),
person AS (
    SELECT
        patient_sk,
        COUNT(*)                                                                    AS record_count,
        MAX(CASE WHEN source = 'mimic' THEN 1 ELSE 0 END)                           AS has_mimic,
        MAX(CASE WHEN source = 'synthea' THEN 1 ELSE 0 END)                         AS has_synthea,
        MAX(CASE WHEN source = 'hl7v2' THEN 1 ELSE 0 END)                           AS has_hl7v2,
        MAX(CASE WHEN is_deceased THEN 1 ELSE 0 END)                                AS any_deceased,
        MIN(CASE WHEN rn = 1 THEN NULL ELSE gender END)                             AS other_gender,
        MIN(CASE WHEN rn = 1 THEN NULL ELSE state END)                              AS other_state
    FROM ranked
    GROUP BY patient_sk
),
representative AS (
    SELECT * FROM ranked WHERE rn = 1
)
SELECT
    p.patient_sk,
    -- sources in a fixed order, comma separated: hl7v2, mimic, synthea
    SUBSTR(CASE WHEN p.has_hl7v2 = 1 THEN ',hl7v2' ELSE '' END
           || CASE WHEN p.has_mimic = 1 THEN ',mimic' ELSE '' END
           || CASE WHEN p.has_synthea = 1 THEN ',synthea' ELSE '' END, 2)              AS source_list,
    CAST(p.has_hl7v2 + p.has_mimic + p.has_synthea AS INTEGER)                        AS source_count,
    CAST(p.record_count AS INTEGER)                                                   AS record_count,
    COALESCE(r.gender, p.other_gender, 'unknown')                                     AS sex,
    CASE WHEN r.age_years >= 90 THEN NULL
         ELSE CAST(EXTRACT(YEAR FROM r.birth_date) AS INTEGER) END                    AS birth_year,
    CASE
        WHEN r.age_years IS NULL THEN 'unknown'
        WHEN r.age_years >= 90 THEN '90+'
        WHEN r.age_years < 18 THEN '0-17'
        ELSE CAST(CAST(FLOOR(r.age_years / 10) * 10 AS INTEGER) AS VARCHAR) || 's'
    END                                                                               AS age_band,
    CASE WHEN p.any_deceased = 1 THEN TRUE ELSE FALSE END                             AS is_deceased,
    COALESCE(r.state, p.other_state)                                                  AS state
FROM person p
JOIN representative r ON r.patient_sk = p.patient_sk
