-- Data quality: when a visit changes beds, the earlier bed entry is completed and ends when the next one starts.
WITH beds AS (
    SELECT
        encounter_id, entry_index, status, end_at,
        LEAD(start_at) OVER (PARTITION BY encounter_id ORDER BY entry_index) AS next_start_at
    FROM {{ ref('silver_encounter_location') }}
    WHERE physical_type = 'bd'
)
SELECT encounter_id, entry_index, status, end_at, next_start_at
FROM beds
WHERE next_start_at IS NOT NULL
  AND (status <> 'completed' OR end_at IS NULL OR end_at <> next_start_at)
