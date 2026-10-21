{# Bronze source: where the raw FHIR resources are read from, per engine. #}
{% macro bronze_select() -%}
  {{ return(adapter.dispatch('bronze_select')()) }}
{%- endmacro %}

{# DuckDB reads the landing folder directly. #}
{% macro duckdb__bronze_select() -%}
SELECT
    ROW_NUMBER() OVER (ORDER BY filename)                      AS bronze_id,
    {{ duckdb__json_str('json', 'resourceType') }}    AS resource_type,
    {{ duckdb__json_str('json', 'id') }}              AS resource_id,
    filename                                                   AS source_file,
    CAST('{{ var("loaded_at") }}' AS TIMESTAMP)                AS loaded_at,
    json                                                       AS raw
FROM read_ndjson_objects('{{ var("landing_path") | replace("\\", "/") | replace("'", "''") }}/*.ndjson', filename = true)
{%- endmacro %}

{# Snowflake queries the landing stage created by sql/snowflake/00_setup_and_load.sql. #}
{% macro snowflake__bronze_select() -%}
SELECT
    ROW_NUMBER() OVER (ORDER BY METADATA$FILENAME)             AS bronze_id,
    $1:resourceType::string                                    AS resource_type,
    $1:id::string                                              AS resource_id,
    METADATA$FILENAME                                          AS source_file,
    CAST('{{ var("loaded_at") }}' AS TIMESTAMP_NTZ)            AS loaded_at,
    $1                                                         AS raw
FROM @{{ var('snowflake_landing_stage') }} (FILE_FORMAT => 'ndjson_format')
{%- endmacro %}
