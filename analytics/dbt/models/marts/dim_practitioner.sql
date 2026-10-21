-- Dimension practitioner: one row per member of staff, plus the Unknown member. Names are kept: they are staff, not
-- patients, and the Riverside General staff are invented (demo). Role, specialty and service come from seed staff_roles
-- for Riverside General; Synthea sends neither, so its clinicians are doctor (when the name has the Dr prefix) with the
-- specialty not recorded.
SELECT
    {{ surrogate_key(['p.practitioner_id']) }}                                        AS practitioner_sk,
    p.practitioner_id,
    p.display_name,
    CASE WHEN r.practitioner_id IS NOT NULL THEN 'riverside reference' ELSE 'synthea' END AS practitioner_source,
    COALESCE(r.role, CASE WHEN p.prefix LIKE 'Dr%' THEN 'doctor' ELSE 'unknown' END)  AS role,
    COALESCE(r.specialty, 'not recorded')                                             AS specialty,
    r.service_name
FROM {{ ref('silver_practitioner') }} p
LEFT JOIN {{ ref('staff_roles') }} r ON r.practitioner_id = p.practitioner_id
UNION ALL
SELECT {{ unknown_key() }}, NULL, 'Unknown', 'unknown', 'unknown', 'not recorded', NULL
