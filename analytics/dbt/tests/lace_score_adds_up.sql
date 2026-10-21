-- The LACE score is the sum of its four parts, each part is in its range, and the band follows the score.
SELECT encounter_id, lace_l, lace_a, lace_c, lace_e, lace_score, risk_band
FROM {{ ref('gold_readmission_risk') }}
WHERE lace_score <> lace_l + lace_a + lace_c + lace_e
   OR lace_l NOT IN (0, 1, 2, 3, 4, 5, 7)
   OR lace_a NOT IN (0, 3)
   OR lace_c NOT IN (0, 1, 2, 3, 5)
   OR lace_e NOT BETWEEN 0 AND 4
   OR risk_band <> CASE WHEN lace_score >= 10 THEN 'high' WHEN lace_score >= 5 THEN 'moderate' ELSE 'low' END
