-- Data quality: a visit never ends before it starts.
{{ config(meta={'check_name': 'encounters do not end before they start', 'check_order': 6}) }}
SELECT encounter_id, start_at, end_at
FROM {{ ref('silver_encounter') }}
WHERE end_at < start_at
