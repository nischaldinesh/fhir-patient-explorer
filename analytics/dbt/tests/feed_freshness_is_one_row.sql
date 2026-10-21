-- gold_feed_freshness always holds exactly one row, even before the live feed has any data.
SELECT COUNT(*) AS n
FROM {{ ref('gold_feed_freshness') }}
HAVING COUNT(*) <> 1
