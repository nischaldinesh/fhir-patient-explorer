{{ config(
    materialized='incremental',
    unique_key='lab_result_sk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}
-- Fact lab result. Grain: one row per numeric lab result (silver_observation, category laboratory, a number and an
-- effective time). Results like <0.01 are kept with their comparator and no range flag.
-- Incremental on the effective date, with a watermark per source: MIMIC dates are shifted into the 2100s, so one
-- watermark for all sources would never let a new live feed result in. Each run reloads the last 3 days of each source
-- (late and corrected results replace their row by key), new sources in full, and every result of a record whose person
-- changed since it was loaded (an EMPI link made or undone moves the record to another master id). A result changed more
-- than 3 days after its effective date, or deleted on the server, needs dbt build --full-refresh.
WITH keys AS (
    SELECT * FROM {{ ref('int_patient_key') }}
),
results AS (
    SELECT
        {{ salted_key(['o.observation_id']) }}                                           AS lab_result_sk,
        k.patient_sk,
        k.patient_record_sk,
        CASE WHEN o.encounter_id IS NOT NULL THEN {{ salted_key(['o.encounter_id']) }} END AS encounter_sk,
        CASE WHEN o.loinc IS NULL THEN {{ unknown_key() }}
             ELSE {{ surrogate_key(['o.loinc']) }} END                                   AS lab_sk,
        {{ date_key('CAST(o.effective_at AS DATE)') }}                                   AS effective_date_key,
        CAST(o.effective_at AS DATE)                                                     AS effective_date,
        o.effective_at,
        k.source,
        o.loinc,
        o.value_num,
        o.comparator,
        o.unit,
        o.ref_low,
        o.ref_high,
        o.range_flag
    FROM {{ ref('silver_observation') }} o
    JOIN keys k ON k.patient_id = o.patient_id
    WHERE o.category = 'laboratory' AND o.value_num IS NOT NULL AND o.effective_at IS NOT NULL
)
SELECT r.*
FROM results r
{% if is_incremental() %}
WHERE r.effective_date >= COALESCE(
          (SELECT MAX(t.effective_date) FROM {{ this }} t WHERE t.source = r.source) - 3,
          DATE '1900-01-01')
   OR r.patient_record_sk IN (
          SELECT t.patient_record_sk
          FROM {{ this }} t
          JOIN keys k ON k.patient_record_sk = t.patient_record_sk
          WHERE t.patient_sk <> k.patient_sk)
{% endif %}
