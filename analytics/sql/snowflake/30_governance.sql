-- Governance (Snowflake): run last, after the Silver, Gold and mart files, as a role that can create roles and policies.
-- Every load recreates the tables (CREATE OR REPLACE drops their grants), so run this file again after each load.
-- Same rules as the dbt governance models (analytics/dbt/models/governance/), enforced natively:
--   * Masking policies on silver_patient hide identifiers from every role except CLINICIAN, whatever
--     query or tool the data is reached through.
--   * ANALYST gets the Gold aggregates, the secure analyst_* views with a pseudonymous patient_key, and the star
--     schema marts (no names, MRNs, addresses or raw ids; keys are salted).
-- The salt lives in governance_settings, created by 00_setup_and_load.sql.

USE SCHEMA fhir_explorer.analytics;

CREATE ROLE IF NOT EXISTS clinician;
CREATE ROLE IF NOT EXISTS analyst;

CREATE MASKING POLICY IF NOT EXISTS mask_identifier AS (val STRING) RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('CLINICIAN') THEN val ELSE '***' END;

CREATE MASKING POLICY IF NOT EXISTS mask_birth_date AS (val DATE) RETURNS DATE ->
    CASE WHEN IS_ROLE_IN_SESSION('CLINICIAN') THEN val ELSE DATE_FROM_PARTS(YEAR(val), 1, 1) END;

-- FORCE replaces a policy left over from an earlier run (the tables are rebuilt on every load).
ALTER TABLE silver_patient MODIFY COLUMN mrn         SET MASKING POLICY mask_identifier FORCE;
ALTER TABLE silver_patient MODIFY COLUMN family_name SET MASKING POLICY mask_identifier FORCE;
ALTER TABLE silver_patient MODIFY COLUMN given_name  SET MASKING POLICY mask_identifier FORCE;
ALTER TABLE silver_patient MODIFY COLUMN city        SET MASKING POLICY mask_identifier FORCE;
ALTER TABLE silver_patient MODIFY COLUMN postal_code SET MASKING POLICY mask_identifier FORCE;
ALTER TABLE silver_patient MODIFY COLUMN birth_date  SET MASKING POLICY mask_birth_date FORCE;

CREATE OR REPLACE SECURE VIEW analyst_patient AS
SELECT
    'P-' || UPPER(SUBSTR(MD5(g.pseudonym_salt || p.patient_id), 1, 10))           AS patient_key,
    p.source,
    p.gender,
    s.age_years IS NOT NULL AND s.age_years >= 90                                  AS age_90_or_over,
    CASE WHEN s.age_years >= 90 THEN NULL ELSE YEAR(p.birth_date) END             AS birth_year,
    CASE
        WHEN s.age_years IS NULL THEN 'unknown'
        WHEN s.age_years >= 90 THEN '90+'
        WHEN s.age_years < 18 THEN '0-17'
        ELSE TO_VARCHAR(FLOOR(s.age_years / 10) * 10) || 's'
    END                                                                            AS age_band,
    p.is_deceased,
    p.state
FROM silver_patient p
JOIN gold_patient_summary s ON s.patient_id = p.patient_id
CROSS JOIN governance_settings g;

CREATE OR REPLACE SECURE VIEW analyst_patient_summary AS
SELECT
    'P-' || UPPER(SUBSTR(MD5(g.pseudonym_salt || s.patient_id), 1, 10)) AS patient_key,
    s.source, s.gender, s.is_deceased,
    CASE WHEN s.age_years >= 90 THEN NULL ELSE s.age_years END          AS age_years,
    s.n_encounters, s.n_admissions, s.inpatient_days, s.readmissions_30d,
    s.n_diagnoses, s.n_diagnoses_icd10, s.n_diagnoses_need_review,
    s.n_lab_results, s.n_lab_out_of_range, s.latest_bmi, s.bmi_category, s.has_aki_signal
FROM gold_patient_summary s
CROSS JOIN governance_settings g;

CREATE OR REPLACE SECURE VIEW analyst_cohort_members AS
SELECT m.cohort, 'P-' || UPPER(SUBSTR(MD5(g.pseudonym_salt || m.patient_id), 1, 10)) AS patient_key
FROM gold_cohort_members m
CROSS JOIN governance_settings g;

CREATE OR REPLACE SECURE VIEW analyst_aki_signal AS
SELECT
    'P-' || UPPER(SUBSTR(MD5(g.pseudonym_salt || k.patient_id), 1, 10)) AS patient_key,
    p.source, k.first_signal_at, k.max_rise_mg_dl, k.n_signal_results
FROM gold_aki_signal k
JOIN silver_patient p ON p.patient_id = k.patient_id
CROSS JOIN governance_settings g;

GRANT USAGE ON DATABASE fhir_explorer TO ROLE clinician;
GRANT USAGE ON DATABASE fhir_explorer TO ROLE analyst;
GRANT USAGE ON SCHEMA fhir_explorer.analytics TO ROLE clinician;
GRANT USAGE ON SCHEMA fhir_explorer.analytics TO ROLE analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA fhir_explorer.analytics TO ROLE clinician;
GRANT SELECT ON ALL VIEWS IN SCHEMA fhir_explorer.analytics TO ROLE clinician;
-- Analysts: Gold aggregates and the pseudonymized views only. No Bronze, no Silver, no settings, and none of the
-- care team worklists (gold_bed_board, gold_readmission_risk), which carry names and MRNs.
GRANT SELECT ON TABLE gold_cohorts             TO ROLE analyst;
GRANT SELECT ON TABLE gold_terminology_quality TO ROLE analyst;
GRANT SELECT ON TABLE gold_weekly_metrics      TO ROLE analyst;
GRANT SELECT ON TABLE gold_daily_census        TO ROLE analyst;
GRANT SELECT ON VIEW analyst_patient           TO ROLE analyst;
GRANT SELECT ON VIEW analyst_patient_summary   TO ROLE analyst;
GRANT SELECT ON VIEW analyst_cohort_members    TO ROLE analyst;
GRANT SELECT ON VIEW analyst_aki_signal        TO ROLE analyst;
-- The star schema (sql/snowflake/marts/): no names, MRNs, addresses below the state, exact birth dates or raw FHIR ids,
-- and every patient level key is salted. Dates of service are kept (a limited data set). Not the internal models
-- (int_patient_key holds FHIR ids, int_patient_demographics and snap_patient_demographics hold addresses,
-- int_quality_events and int_quality_cells hold the unsuppressed small counts).
GRANT SELECT ON TABLE dim_date                 TO ROLE analyst;
GRANT SELECT ON TABLE dim_patient              TO ROLE analyst;
GRANT SELECT ON VIEW dim_patient_history       TO ROLE analyst;
GRANT SELECT ON TABLE dim_location             TO ROLE analyst;
GRANT SELECT ON TABLE dim_practitioner         TO ROLE analyst;
GRANT SELECT ON TABLE dim_diagnosis            TO ROLE analyst;
GRANT SELECT ON TABLE dim_lab                  TO ROLE analyst;
GRANT SELECT ON TABLE fact_encounter           TO ROLE analyst;
GRANT SELECT ON TABLE fact_diagnosis           TO ROLE analyst;
GRANT SELECT ON TABLE fact_lab_result          TO ROLE analyst;
GRANT SELECT ON TABLE fact_bed_day             TO ROLE analyst;
GRANT SELECT ON TABLE fact_bed_capacity_day    TO ROLE analyst;
GRANT SELECT ON TABLE mart_quality_monthly     TO ROLE analyst;
GRANT SELECT ON TABLE mart_quality_period      TO ROLE analyst;
GRANT SELECT ON TABLE mart_suppression_report  TO ROLE analyst;
