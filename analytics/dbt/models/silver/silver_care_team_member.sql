-- Silver care team member: one row per CareTeam.participant, so the care manager of a visit can be found.
-- The Riverside General feed sends one CareTeam per visit (attending, primary nurse, care manager from discharge).
WITH latest AS (
    SELECT *
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'CareTeam'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
)
SELECT
    l.resource_id                                                                       AS care_team_id,
    {{ json_row_index('p') }}                                                           AS member_index,
    REPLACE({{ json_str('l.raw', 'encounter.reference') }}, 'Encounter/', '')                   AS encounter_id,
    REPLACE({{ json_str('l.raw', 'subject.reference') }}, 'Patient/', '')                       AS patient_id,
    {{ json_str('l.raw', 'status') }}                                                           AS care_team_status,
    {{ json_str('p.value', 'role[0].coding[0].code') }}                                         AS role_code,
    REPLACE({{ json_str('p.value', 'member.reference') }}, 'PractitionerRole/', '')             AS member_role_id,
    {{ json_str('p.value', 'member.display') }}                                                 AS member_name,
    {{ json_ts('p.value', 'period.start') }}                                                 AS start_at,
    {{ json_ts('p.value', 'period.end') }}                                                   AS end_at
FROM latest l {{ json_rows('l.raw', 'participant', 'p') }}
