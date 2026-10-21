-- Governance (DuckDB): role based views. DuckDB has no users or masking policies, so each role gets its own
-- views; in Snowflake the same rules are enforced natively with masking policies (sql/snowflake/30_governance.sql).
--
--   clinician  sees everything (silver_* and gold_* tables)
--   analyst    sees only the analyst_* views: a stable pseudonym instead of the patient id, no name, MRN,
--              city, postal code or full birth date. Following HIPAA Safe Harbor, ages 90 and over are
--              grouped and their birth year is withheld.
SELECT * FROM (VALUES
    ('clinician', TRUE,  'Treats patients: full identifiers'),
    ('analyst',   FALSE, 'Population analytics: pseudonymized, minimum necessary')
) AS t(role, sees_identifiers, purpose)
