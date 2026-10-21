-- Gold patient crosswalk: one row per patient record with one master id per person across sources. Records joined by
-- Patient.link (written by the EMPI, empi/match.py) are one person; following links up to 10 steps, the group's
-- representative is its smallest patient id and its master id a stable hash of it. A record without links is a
-- person of its own.
WITH RECURSIVE edges AS (
    SELECT l.patient_id AS from_id, l.other_patient_id AS to_id
    FROM {{ ref('silver_patient_link') }} l
    JOIN {{ ref('silver_patient') }} o ON o.patient_id = l.other_patient_id
    WHERE l.link_type IN ('seealso', 'refer', 'replaced-by', 'replaces')
    UNION
    SELECT l.other_patient_id, l.patient_id
    FROM {{ ref('silver_patient_link') }} l
    JOIN {{ ref('silver_patient') }} o ON o.patient_id = l.other_patient_id
    WHERE l.link_type IN ('seealso', 'refer', 'replaced-by', 'replaces')
),
reach AS (
    SELECT patient_id, patient_id AS member_id, 0 AS depth
    FROM {{ ref('silver_patient') }}
    UNION ALL
    SELECT r.patient_id, e.to_id, r.depth + 1
    FROM reach r
    JOIN edges e ON e.from_id = r.member_id
    WHERE r.depth < 10
),
grouped AS (
    SELECT patient_id, MIN(member_id) AS master_patient_id, COUNT(DISTINCT member_id) AS records_in_group
    FROM reach
    GROUP BY patient_id
),
group_sources AS (
    SELECT g.master_patient_id, COUNT(DISTINCT p.source) AS sources_in_group
    FROM grouped g
    JOIN {{ ref('silver_patient') }} p ON p.patient_id = g.patient_id
    GROUP BY g.master_patient_id
)
SELECT
    'MPI-' || UPPER(SUBSTR(MD5(g.master_patient_id), 1, 10))   AS master_id,
    g.master_patient_id,
    p.patient_id,
    p.source,
    p.mrn,
    g.records_in_group,
    s.sources_in_group
FROM grouped g
JOIN {{ ref('silver_patient') }} p ON p.patient_id = g.patient_id
JOIN group_sources s ON s.master_patient_id = g.master_patient_id
