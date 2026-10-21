-- Snowflake: setup, Bronze and seeds. Run once per load, then the files in snowflake/silver/, snowflake/gold/ and
-- snowflake/marts/, in order, and finally 30_governance.sql (policies and grants on what was just built).
-- The Silver, Gold and mart files are generated from the dbt models in analytics/dbt/ by `python3 analytics/build.py render`; do not edit them by hand.
--
-- Upload the landing files and seeds first, with SnowSQL from the project folder:
--   PUT file://data/analytics/landing/*.ndjson @fhir_landing AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://terminology/lab_loinc_map.csv     @fhir_seeds   AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://terminology/out/icd9_to_icd10.csv @fhir_seeds   AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://analytics/dbt/seeds/key_labs.csv     @fhir_seeds   AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://analytics/dbt/seeds/cohort_codes.csv @fhir_seeds   AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://analytics/dbt/seeds/charlson_codes.csv @fhir_seeds AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://analytics/dbt/seeds/unmapped_labs.csv  @fhir_seeds AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://analytics/dbt/seeds/icd10_chapters.csv @fhir_seeds AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://analytics/dbt/seeds/staff_roles.csv    @fhir_seeds AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
--   PUT file://analytics/dbt/seeds/quality_measures.csv @fhir_seeds AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
-- (Or upload the same files in the Snowsight UI: Data > Add data > Load files into a stage.)

CREATE DATABASE IF NOT EXISTS fhir_explorer;
CREATE SCHEMA IF NOT EXISTS fhir_explorer.analytics;
USE SCHEMA fhir_explorer.analytics;

CREATE FILE FORMAT IF NOT EXISTS ndjson_format TYPE = JSON STRIP_OUTER_ARRAY = FALSE;
CREATE FILE FORMAT IF NOT EXISTS csv_format TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';
CREATE STAGE IF NOT EXISTS fhir_landing FILE_FORMAT = ndjson_format;
CREATE STAGE IF NOT EXISTS fhir_seeds FILE_FORMAT = csv_format;

-- The pseudonym salt, read by the marts' salted keys and the analyst views (30_governance.sql). Replace the value with a
-- secret before loading real data, the same as PSEUDONYM_SALT in DuckDB, so keys and pseudonyms match in both engines.
-- No role but the owner and CLINICIAN (all tables) can read it; ANALYST is never granted it.
CREATE OR REPLACE TABLE governance_settings AS SELECT 'local-demo-salt' AS pseudonym_salt;

-- Bronze: every FHIR resource exactly as extracted, as VARIANT, plus lineage. Nothing is transformed here.
CREATE OR REPLACE TABLE bronze_resources (
    bronze_id      NUMBER AUTOINCREMENT,
    resource_type  STRING,
    resource_id    STRING,
    source_file    STRING,
    loaded_at      TIMESTAMP_NTZ,
    raw            VARIANT
);

COPY INTO bronze_resources (resource_type, resource_id, source_file, loaded_at, raw)
FROM (
    SELECT $1:resourceType::string, $1:id::string, METADATA$FILENAME, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, $1
    FROM @fhir_landing
)
FILE_FORMAT = (FORMAT_NAME = ndjson_format);

-- Seeds: the curated mapping tables from terminology/
CREATE OR REPLACE TABLE seed_lab_loinc (
    mimic_code STRING, mimic_display STRING, loinc STRING, loinc_display STRING,
    unit STRING, equivalence STRING, note STRING
);
COPY INTO seed_lab_loinc FROM @fhir_seeds/lab_loinc_map.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);

CREATE OR REPLACE TABLE seed_icd9_icd10 (
    icd9 STRING, icd9_display STRING, rows STRING, category STRING,
    needs_review STRING, icd10 STRING, icd10_display STRING
);
COPY INTO seed_icd9_icd10 FROM @fhir_seeds/icd9_to_icd10.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);

CREATE OR REPLACE TABLE seed_key_labs (loinc STRING, lab STRING, unit STRING, note STRING);
COPY INTO seed_key_labs FROM @fhir_seeds/key_labs.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);

CREATE OR REPLACE TABLE seed_cohort_codes (cohort STRING, code_system STRING, code_pattern STRING, description STRING);
COPY INTO seed_cohort_codes FROM @fhir_seeds/cohort_codes.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);

CREATE OR REPLACE TABLE seed_charlson_codes (
    category STRING, weight INTEGER, superseded_by STRING, code_system STRING, code_prefix STRING, description STRING
);
COPY INTO seed_charlson_codes FROM @fhir_seeds/charlson_codes.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);

CREATE OR REPLACE TABLE seed_unmapped_labs (match_type STRING, match STRING, reason STRING);
COPY INTO seed_unmapped_labs FROM @fhir_seeds/unmapped_labs.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);

-- Seeds for the marts: ICD 10 CM chapters (dim_diagnosis), the Riverside General staff roles (dim_practitioner, demo)
-- and the quality measures (mart_quality_monthly, mart_quality_period)
CREATE OR REPLACE TABLE seed_icd10_chapters (chapter INTEGER, first_category STRING, last_category STRING, title STRING);
COPY INTO seed_icd10_chapters FROM @fhir_seeds/icd10_chapters.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);

CREATE OR REPLACE TABLE seed_staff_roles (practitioner_id STRING, role STRING, specialty STRING, service_name STRING);
COPY INTO seed_staff_roles FROM @fhir_seeds/staff_roles.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);

CREATE OR REPLACE TABLE seed_quality_measures (measure_id STRING, measure_name STRING, kind STRING, unit STRING, counts STRING);
COPY INTO seed_quality_measures FROM @fhir_seeds/quality_measures.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format);
