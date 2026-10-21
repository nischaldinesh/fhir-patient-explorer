-- Gold feed freshness: one row saying how current the live feed data is. The nightly job loads yesterday, so the
-- data date (the "today" of the operational pages) is the day of the last feed event, never later than the extract.
-- All columns are NULL when the server has no live feed yet.
WITH live AS (
    SELECT encounter_id, start_at, end_at FROM {{ ref('gold_live_encounter') }}
),
events AS (
    SELECT start_at AS event_at FROM live
    UNION ALL
    SELECT end_at FROM live WHERE end_at IS NOT NULL
    UNION ALL
    SELECT o.effective_at FROM {{ ref('silver_observation') }} o JOIN live ON live.encounter_id = o.encounter_id
),
extract_run AS (
    SELECT MAX(loaded_at) AS extracted_at FROM {{ ref('bronze_resources') }}
),
last_event AS (
    SELECT MIN(event_at) AS first_event_at, MAX(event_at) AS last_event_at FROM events
)
SELECT
    CAST(e.first_event_at AS DATE)                                                 AS first_day,
    -- A manual run for today also writes events later in the day than the run itself: count nothing after the extract
    CASE WHEN e.last_event_at > x.extracted_at THEN x.extracted_at ELSE e.last_event_at END   AS last_event_at,
    CASE WHEN e.last_event_at IS NULL THEN NULL
         WHEN e.last_event_at > x.extracted_at THEN CAST(x.extracted_at AS DATE)
         ELSE CAST(e.last_event_at AS DATE) END                                    AS data_date,
    x.extracted_at,
    CAST(x.extracted_at AS DATE)                                                   AS extract_date
FROM last_event e
CROSS JOIN extract_run x
