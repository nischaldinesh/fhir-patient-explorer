-- Gold encounter summary: length of stay and 30 day readmissions.
-- What counts as a hospital admission depends on the source:
--   * Synthea and HL7 v2: an inpatient class (IMP, ACUTE, NONAC).
--   * MIMIC: the class is the admission TYPE (EMER, AMB elective, OBSENC observation, SS same day surgery),
--     not whether the patient was admitted. Hospital stays, ED visits and ICU stays are told apart by their
--     identifier system instead (encounter-hosp, encounter-ed, encounter-icu).
--   * Never an encounter that is part of another one (FHIR partOf), such as an ICU stay inside a hospital stay.
-- Readmission = the patient's next admission starts within 30 days after this one ended.
-- Who and where (the Riverside General feed; NULL for sources that do not send them): the attending physician,
-- the last unit and bed, and how many times the bed changed during the visit.
WITH e AS (
    SELECT
        e.*,
        p.source,
        CASE
            WHEN e.part_of_encounter_id IS NOT NULL THEN 'part of another stay'
            WHEN e.identifier_system LIKE '%/encounter-hosp' THEN 'hospital admission'
            WHEN e.identifier_system LIKE '%/encounter-ed' THEN 'emergency visit'
            WHEN e.identifier_system LIKE '%mimic%' THEN 'other'
            WHEN e.class_code IN ('IMP', 'ACUTE', 'NONAC') THEN 'hospital admission'
            WHEN e.class_code = 'EMER' THEN 'emergency visit'
            ELSE 'other'
        END AS encounter_kind
    FROM {{ ref('silver_encounter') }} e
    JOIN {{ ref('silver_patient') }} p ON p.patient_id = e.patient_id
),
k AS (
    SELECT e.*, CASE WHEN encounter_kind = 'hospital admission' THEN TRUE ELSE FALSE END AS is_admission
    FROM e
),
admissions AS (
    SELECT
        encounter_id,
        LEAD(start_at) OVER (PARTITION BY patient_id ORDER BY start_at, encounter_id) AS next_admission_at
    FROM k
    WHERE is_admission
)
SELECT
    e.encounter_id,
    e.patient_id,
    e.source,
    e.class_code,
    e.encounter_type,
    e.status,
    e.start_at,
    e.end_at,
    e.encounter_kind,
    e.is_admission,
    CASE WHEN e.end_at IS NOT NULL THEN ROUND(DATEDIFF('minute', e.start_at, e.end_at) / 1440.0, 2) END AS length_of_stay_days,
    a.next_admission_at,
    CASE WHEN a.next_admission_at >= e.end_at
         THEN ROUND(DATEDIFF('minute', e.end_at, a.next_admission_at) / 1440.0, 1) END       AS days_to_next_admission,
    CASE WHEN e.is_admission AND a.next_admission_at >= e.end_at
          AND DATEDIFF('minute', e.end_at, a.next_admission_at) <= 30 * 1440 THEN TRUE ELSE FALSE END AS readmitted_30d,
    e.attending_practitioner_id,
    pr.display_name                                                                           AS attending_name,
    e.last_location_id,
    loc.name                                                                                  AS last_location_name,
    unit.name                                                                                 AS last_unit_name,
    e.bed_moves
FROM k e
LEFT JOIN admissions a ON a.encounter_id = e.encounter_id
LEFT JOIN {{ ref('silver_practitioner') }} pr ON pr.practitioner_id = e.attending_practitioner_id
LEFT JOIN {{ ref('silver_location') }} loc ON loc.location_id = e.last_location_id
LEFT JOIN {{ ref('silver_location') }} unit ON unit.location_id = loc.unit_location_id
