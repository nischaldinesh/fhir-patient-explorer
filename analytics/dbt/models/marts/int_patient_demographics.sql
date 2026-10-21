-- Internal: the demographics the snapshot snap_patient_demographics tracks (SCD type 2), one row per patient record,
-- as of the extract (loaded_at). Address, sex and the deceased flag; the address is identifying, so this model and the
-- snapshot are for building the marts only. dim_patient_history shows the history without the address itself.
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Patient'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
)
SELECT
    p.patient_id,
    p.source,
    {{ json_str('l.raw', 'address[0].line[0]') }}          AS address_line,
    p.city,
    p.state,
    p.postal_code,
    p.gender                                               AS sex,
    p.is_deceased,
    p.loaded_at
FROM {{ ref('silver_patient') }} p
JOIN latest l ON l.resource_id = p.patient_id
