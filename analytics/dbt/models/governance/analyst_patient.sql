-- Analyst view of patients: pseudonym, no identifiers, age bands with 90 and over grouped (HIPAA Safe Harbor).
SELECT
    {{ patient_key('p.patient_id') }}                    AS patient_key,
    p.source,
    p.gender,
    s.age_years IS NOT NULL AND s.age_years >= 90                                  AS age_90_or_over,
    CASE WHEN s.age_years >= 90 THEN NULL ELSE EXTRACT(YEAR FROM p.birth_date) END AS birth_year,
    CASE
        WHEN s.age_years IS NULL THEN 'unknown'
        WHEN s.age_years >= 90 THEN '90+'
        WHEN s.age_years < 18 THEN '0-17'
        ELSE CAST(CAST(FLOOR(s.age_years / 10) * 10 AS INTEGER) AS VARCHAR) || 's'
    END                                                                            AS age_band,
    p.is_deceased,
    p.state
FROM {{ ref('silver_patient') }} p
JOIN {{ ref('gold_patient_summary') }} s ON s.patient_id = p.patient_id
