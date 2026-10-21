-- Dimension patient history: the SCD type 2 versions of snap_patient_demographics, one row per patient record and
-- version, with valid_from, valid_to and is_current. Sex, the deceased flag and the state are shown; the address itself
-- is not (it is identifying), only its version number and whether it changed with this version. A view, so it always
-- reflects the snapshot.
WITH versions AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (PARTITION BY s.patient_id ORDER BY s.dbt_valid_from)                            AS version_number,
        LAG(COALESCE(s.address_line, '') || '|' || COALESCE(s.city, '') || '|' || COALESCE(s.state, '') || '|'
            || COALESCE(s.postal_code, '')) OVER (PARTITION BY s.patient_id ORDER BY s.dbt_valid_from)      AS previous_address,
        COALESCE(s.address_line, '') || '|' || COALESCE(s.city, '') || '|' || COALESCE(s.state, '') || '|'
            || COALESCE(s.postal_code, '')                                                                 AS address
    FROM {{ ref('snap_patient_demographics') }} s
),
numbered AS (
    SELECT
        v.*,
        CASE WHEN v.previous_address IS NOT NULL AND v.previous_address <> v.address THEN 1 ELSE 0 END    AS address_changed_flag
    FROM versions v
)
SELECT
    {{ salted_key(['n.patient_id', 'n.dbt_valid_from']) }}                                                AS patient_version_sk,
    k.patient_sk,
    {{ salted_key(['n.patient_id']) }}                                                                   AS patient_record_sk,
    n.source,
    CAST(n.version_number AS INTEGER)                                                                    AS version_number,
    COALESCE(n.sex, 'unknown')                                                                           AS sex,
    n.is_deceased,
    n.state,
    CAST(1 + SUM(n.address_changed_flag) OVER (PARTITION BY n.patient_id ORDER BY n.dbt_valid_from
                                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INTEGER) AS address_version,
    CASE WHEN n.address_changed_flag = 1 THEN TRUE ELSE FALSE END                                       AS address_changed,
    CAST(n.dbt_valid_from AS TIMESTAMP)                                                                  AS valid_from,
    CAST(n.dbt_valid_to AS TIMESTAMP)                                                                    AS valid_to,
    CASE WHEN n.dbt_valid_to IS NULL THEN TRUE ELSE FALSE END                                            AS is_current
FROM numbered n
LEFT JOIN {{ ref('int_patient_key') }} k ON k.patient_id = n.patient_id
