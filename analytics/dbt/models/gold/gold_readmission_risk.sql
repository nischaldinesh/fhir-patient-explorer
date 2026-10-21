-- Gold readmission risk: the LACE index (van Walraven et al., CMAJ 2010) for every inpatient discharge of the live
-- Riverside General feed in the 30 days up to the extract date, the care manager's follow up list.
--   L  length of stay in whole days: under 1 day 0, 1 day 1, 2 days 2, 3 days 3, 4 to 6 days 4, 7 to 13 days 5,
--      14 or more 7
--   A  acute (emergency) admission, Encounter.priority EM from HL7 v2 PV1-4: 3
--   C  Charlson comorbidity score from the patient's diagnoses up to this discharge (seed charlson_codes, Quan 2005
--      ICD 10 coding; within a condition family only the more severe form counts): 0 to 3 as is, 4 or more 5
--   E  emergency department visits in the 6 months (182 days) before the admission, capped at 4
--   Risk band: 10 or more high, 5 to 9 moderate, under 5 low.
-- Transitional Care Management (CMS codes 99495 and 99496) timing, in calendar dates:
--   contact due within 2 business days of discharge (weekends skipped, public holidays not known here)
--   follow up visit due within 7 days for high risk, 14 days otherwise
--   overdue: the data date is past the due date. The feed has no record of calls or visits, so this only says the
--   date has passed, not that nothing happened.
-- Suggested service: Cardiology Clinic for heart failure (the heart_failure cohort), else Discharge Follow Up Clinic.
-- Identifying (patient name, MRN): for the care team, never for analysts.
WITH fresh AS (
    SELECT data_date, extract_date FROM {{ ref('gold_feed_freshness') }}
),
discharges AS (
    SELECT e.*
    FROM {{ ref('gold_live_encounter') }} e
    CROSS JOIN fresh f
    WHERE e.is_admission AND e.end_date IS NOT NULL
      AND DATEDIFF('day', e.end_date, f.extract_date) BETWEEN 0 AND 30
),
charlson_hits AS (
    -- every Charlson category a diagnosis of the patient falls in, recorded up to this discharge
    SELECT DISTINCT d.encounter_id, s.category, s.weight, s.superseded_by
    FROM discharges d
    JOIN {{ ref('silver_condition') }} c
      ON c.patient_id = d.patient_id
     AND (c.encounter_id = d.encounter_id OR c.recorded_at <= d.end_at)
    JOIN {{ ref('charlson_codes') }} s
      ON (s.code_system = 'icd10' AND c.icd10_code LIKE s.code_prefix || '%')
      OR (s.code_system = 'icd9' AND c.code_system = 'icd9' AND REPLACE(c.code, '.', '') LIKE s.code_prefix || '%')
),
charlson AS (
    SELECT h.encounter_id, SUM(h.weight) AS charlson_score
    FROM charlson_hits h
    WHERE NOT EXISTS (
        SELECT 1 FROM charlson_hits worse
        WHERE worse.encounter_id = h.encounter_id AND worse.category = h.superseded_by
    )
    GROUP BY h.encounter_id
),
ed AS (
    SELECT d.encounter_id, COUNT(*) AS ed_visits_6m
    FROM discharges d
    JOIN {{ ref('gold_live_encounter') }} v
      ON v.patient_id = d.patient_id
     AND NOT v.is_admission
     AND v.start_at < d.start_at
     AND DATEDIFF('day', v.start_at, d.start_at) <= 182
    GROUP BY d.encounter_id
),
care_manager AS (
    SELECT encounter_id, member_name, member_role_id,
           ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY start_at DESC, member_index DESC) AS rn
    FROM {{ ref('silver_care_team_member') }}
    WHERE role_code = 'care-manager'
),
heart_failure AS (
    SELECT DISTINCT patient_id FROM {{ ref('gold_cohort_members') }} WHERE cohort = 'heart_failure'
),
scored AS (
    SELECT
        d.*,
        CASE
            WHEN d.length_of_stay_days < 1 THEN 0
            WHEN d.length_of_stay_days < 2 THEN 1
            WHEN d.length_of_stay_days < 3 THEN 2
            WHEN d.length_of_stay_days < 4 THEN 3
            WHEN d.length_of_stay_days < 7 THEN 4
            WHEN d.length_of_stay_days < 14 THEN 5
            ELSE 7
        END                                                                   AS lace_l,
        CASE WHEN d.is_emergency_admission THEN 3 ELSE 0 END                  AS lace_a,
        COALESCE(c.charlson_score, 0)                                         AS charlson_score,
        CASE WHEN COALESCE(c.charlson_score, 0) >= 4 THEN 5 ELSE COALESCE(c.charlson_score, 0) END AS lace_c,
        COALESCE(e.ed_visits_6m, 0)                                           AS ed_visits_6m,
        LEAST(COALESCE(e.ed_visits_6m, 0), 4)                                 AS lace_e,
        CASE WHEN hf.patient_id IS NOT NULL THEN TRUE ELSE FALSE END          AS has_heart_failure
    FROM discharges d
    LEFT JOIN charlson c       ON c.encounter_id = d.encounter_id
    LEFT JOIN ed e             ON e.encounter_id = d.encounter_id
    LEFT JOIN heart_failure hf ON hf.patient_id = d.patient_id
),
banded AS (
    SELECT
        s.*,
        s.lace_l + s.lace_a + s.lace_c + s.lace_e                             AS lace_score,
        CASE
            WHEN s.lace_l + s.lace_a + s.lace_c + s.lace_e >= 10 THEN 'high'
            WHEN s.lace_l + s.lace_a + s.lace_c + s.lace_e >= 5 THEN 'moderate'
            ELSE 'low'
        END                                                                   AS risk_band
    FROM scored s
),
timed AS (
    SELECT
        b.*,
        {{ add_business_days('b.end_date', 2) }}                              AS contact_due_date,
        CASE WHEN b.risk_band = 'high' THEN 7 ELSE 14 END                     AS follow_up_within_days
    FROM banded b
)
SELECT
    t.encounter_id,
    t.visit_number,
    t.patient_id,
    NULLIF(TRIM(COALESCE(p.given_name || ' ', '') || COALESCE(p.family_name, '')), '')   AS patient_name,
    p.mrn,
    t.start_at                                                                AS admit_at,
    t.end_at                                                                  AS discharge_at,
    t.end_date                                                                AS discharge_date,
    t.last_unit_name,
    t.last_location_name,
    t.attending_name,
    t.length_of_stay_days,
    t.lace_l,
    t.is_emergency_admission,
    t.lace_a,
    t.charlson_score,
    t.lace_c,
    t.ed_visits_6m,
    t.lace_e,
    t.lace_score,
    t.risk_band,
    DATEDIFF('day', t.end_date, f.data_date)                                  AS days_since_discharge,
    t.readmitted_30d                                                          AS readmitted,
    CASE WHEN t.readmitted_30d THEN t.next_admission_at END                   AS readmitted_at,
    cm.member_name                                                            AS care_manager_name,
    cm.member_role_id                                                         AS care_manager_role_id,
    t.contact_due_date,
    CASE WHEN f.data_date > t.contact_due_date THEN TRUE ELSE FALSE END       AS contact_overdue,
    t.follow_up_within_days,
    t.end_date + t.follow_up_within_days                                      AS visit_due_date,
    CASE WHEN f.data_date > t.end_date + t.follow_up_within_days THEN TRUE ELSE FALSE END AS visit_overdue,
    t.has_heart_failure,
    CASE WHEN t.has_heart_failure THEN 'riverside-hcs-cardiology-clinic'
         ELSE 'riverside-hcs-discharge-follow-up' END                         AS suggested_service_id,
    CASE WHEN t.has_heart_failure THEN 'Cardiology Clinic'
         ELSE 'Discharge Follow Up Clinic' END                                AS suggested_service_name
FROM timed t
CROSS JOIN fresh f
LEFT JOIN care_manager cm                  ON cm.encounter_id = t.encounter_id AND cm.rn = 1
LEFT JOIN {{ ref('silver_patient') }} p    ON p.patient_id = t.patient_id
