-- Internal: one row per patient record with its salted keys. patient_record_sk stands for the record, patient_sk for
-- the person (the master id of gold_patient_crosswalk, so records the EMPI linked share it). Every mart takes its
-- patient keys from here. Holds the FHIR patient id, so it is for building the marts only, never for BI users.
SELECT
    x.patient_id,
    x.source,
    x.master_id,
    {{ salted_key(['x.patient_id']) }}     AS patient_record_sk,
    {{ salted_key(['x.master_id']) }}      AS patient_sk
FROM {{ ref('gold_patient_crosswalk') }} x
