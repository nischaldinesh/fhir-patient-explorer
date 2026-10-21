-- gold_empi_quality is one row whose counts add up: automatic links are true or false, true matches are found or not.
SELECT *
FROM {{ ref('gold_empi_quality') }}
WHERE true_positives + false_positives <> auto_linked
   OR true_positives + false_negatives <> true_match_pairs
   OR false_positives < 0 OR false_negatives < 0
   OR (SELECT COUNT(*) FROM {{ ref('gold_empi_quality') }}) <> 1
