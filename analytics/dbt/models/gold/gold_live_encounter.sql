-- Gold live encounter: the hospital admissions and emergency visits of the live Riverside General feed (demo,
-- invented patients on real dates), the base of the operational tables (census, bed board, weekly metrics, LACE).
-- The live feed is the HL7 v2 source with visit numbers that carry the date, V<yyyymmdd><nn> (hl7v2/generate.py).
-- The original 25 patient feed (visit numbers V70000 and up) is a static archive, like MIMIC and Synthea, and is
-- left out, as is everything from those two sources.
WITH live AS (
    SELECT s.*, e.identifier_value, e.priority_code
    FROM {{ ref('gold_encounter_summary') }} s
    JOIN {{ ref('silver_encounter') }} e ON e.encounter_id = s.encounter_id
    WHERE s.source = 'hl7v2'
      AND e.identifier_value LIKE 'V__________'
      AND s.encounter_kind IN ('hospital admission', 'emergency visit')
),
kidney AS (
    SELECT encounter_id, MIN(signal_at) AS first_kidney_signal_at
    FROM {{ ref('gold_aki_rise') }}
    GROUP BY encounter_id
)
SELECT
    l.encounter_id,
    l.patient_id,
    l.identifier_value                                                        AS visit_number,
    l.encounter_kind,
    l.is_admission,
    l.priority_code,
    CASE WHEN l.is_admission AND l.priority_code = 'EM' THEN TRUE ELSE FALSE END AS is_emergency_admission,
    l.start_at,
    l.end_at,
    CAST(l.start_at AS DATE)                                                  AS start_date,
    CAST(l.end_at AS DATE)                                                    AS end_date,
    l.length_of_stay_days,
    l.next_admission_at,
    l.readmitted_30d,
    l.attending_practitioner_id,
    l.attending_name,
    l.last_location_id,
    l.last_location_name,
    l.last_unit_name,
    l.bed_moves,
    k.first_kidney_signal_at,
    CASE WHEN k.encounter_id IS NOT NULL THEN TRUE ELSE FALSE END             AS has_kidney_signal
FROM live l
LEFT JOIN kidney k ON k.encounter_id = l.encounter_id
