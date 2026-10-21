-- Silver EMPI truth: the ground truth of the seeded EMPI test set (Binary/empi-truth, from hl7v2/pipeline.py
-- empi-seed): which Riverside General registrations are copies of Synthea patients, and which only share a name.
WITH doc AS (
    SELECT {{ json_from_base64(json_str('raw', 'data')) }} AS doc
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Binary' AND resource_id = 'empi-truth'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
)
SELECT
    {{ json_str('t.value', 'seed_id') }}                   AS seed_patient_id,
    {{ json_str('t.value', 'existing_id') }}               AS existing_patient_id,
    {{ json_str('t.value', 'kind') }}                      AS kind,
    {{ json_str('t.value', 'variations') }}                AS variations,
    {{ json_str('t.value', 'note') }}                      AS note
FROM doc d {{ json_rows('d.doc', 'pairs', 't') }}
