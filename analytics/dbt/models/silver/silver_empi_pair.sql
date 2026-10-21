-- Silver EMPI pair: one row per pair in the EMPI review queue (Binary/empi-review, published by empi/match.py run):
-- the score and what was decided. Clear no matches are not published, so they are not here either.
WITH doc AS (
    SELECT {{ json_from_base64(json_str('raw', 'data')) }} AS doc
    FROM {{ ref('bronze_resources') }}
    WHERE resource_type = 'Binary' AND resource_id = 'empi-review'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY loaded_at DESC, bronze_id DESC) = 1
)
SELECT
    {{ json_str('p.value', 'pair_id') }}                   AS pair_id,
    {{ json_str('p.value', 'a.id') }}                      AS patient_a_id,
    {{ json_str('p.value', 'b.id') }}                      AS patient_b_id,
    {{ json_str('p.value', 'a.source') }}                  AS source_a,
    {{ json_str('p.value', 'b.source') }}                  AS source_b,
    {{ json_num('p.value', 'score') }}                     AS score,
    {{ json_str('p.value', 'status') }}                    AS status,
    {{ json_ts('p.value', 'decided_at') }}                 AS decided_at
FROM doc d {{ json_rows('d.doc', 'pairs', 'p') }}
