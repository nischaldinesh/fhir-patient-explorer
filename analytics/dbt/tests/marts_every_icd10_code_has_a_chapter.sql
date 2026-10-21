-- Every ICD 10 code of dim_diagnosis falls in one ICD 10 CM chapter (seed icd10_chapters), and in only one.
SELECT d.icd10_code, COUNT(c.chapter) AS chapters
FROM {{ ref('dim_diagnosis') }} d
LEFT JOIN {{ ref('icd10_chapters') }} c ON d.category_3char BETWEEN c.first_category AND c.last_category
WHERE d.icd10_code IS NOT NULL
GROUP BY d.icd10_code
HAVING COUNT(c.chapter) <> 1
