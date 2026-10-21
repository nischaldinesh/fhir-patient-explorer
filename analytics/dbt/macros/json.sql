{#
  Read one field out of a FHIR JSON document. The only real difference between the engines in this project:

    json_str(col, 'code.coding[0].code')     a string
    json_num(col, 'valueQuantity.value')     a number (NULL when it is not a number)
    json_ts(col, 'effectiveDateTime')        a timestamp, from the first 19 characters (YYYY-MM-DDTHH:MM:SS)

    DuckDB:    json_extract_string(raw, '$.code.coding[0].code')
    Snowflake: raw:code.coding[0].code::string

  Timestamps keep the clock time as sent and drop the time zone offset. MIMIC dates are shifted for privacy
  anyway, and dropping the offset the same way in both engines keeps results identical.

  Each macro dispatches on the adapter (duckdb__ or snowflake__). `build.py render` sets the var
  render_dialect to write the Snowflake SQL offline, without a Snowflake account or dbt-snowflake.
#}

{% macro json_dialect() -%}
  {%- set d = var('render_dialect', none) -%}
  {%- if d is not none and d not in ('duckdb', 'snowflake') -%}
    {{ exceptions.raise_compiler_error("unknown render_dialect " ~ d ~ ": use duckdb or snowflake") }}
  {%- endif -%}
  {{ return(d) }}
{%- endmacro %}

{% macro json_path(path) -%}
  {%- if not modules.re.fullmatch('[][A-Za-z0-9_.]+', path) -%}
    {{ exceptions.raise_compiler_error("bad JSON path " ~ path ~ ": letters, digits, _, . and [n] only") }}
  {%- endif -%}
  {{ return(path) }}
{%- endmacro %}

{% macro json_str(col, path) -%}
  {%- set d = json_dialect() -%}
  {%- if d == 'snowflake' -%}{{ return(snowflake__json_str(col, path)) }}
  {%- elif d == 'duckdb' -%}{{ return(duckdb__json_str(col, path)) }}
  {%- endif -%}
  {{ return(adapter.dispatch('json_str')(col, path)) }}
{%- endmacro %}

{% macro json_num(col, path) -%}
  {%- set d = json_dialect() -%}
  {%- if d == 'snowflake' -%}{{ return(snowflake__json_num(col, path)) }}
  {%- elif d == 'duckdb' -%}{{ return(duckdb__json_num(col, path)) }}
  {%- endif -%}
  {{ return(adapter.dispatch('json_num')(col, path)) }}
{%- endmacro %}

{% macro json_ts(col, path) -%}
  {%- set d = json_dialect() -%}
  {%- if d == 'snowflake' -%}{{ return(snowflake__json_ts(col, path)) }}
  {%- elif d == 'duckdb' -%}{{ return(duckdb__json_ts(col, path)) }}
  {%- endif -%}
  {{ return(adapter.dispatch('json_ts')(col, path)) }}
{%- endmacro %}

{% macro duckdb__json_str(col, path) -%}
  json_extract_string({{ col }}, '$.{{ json_path(path) }}')
{%- endmacro %}

{% macro snowflake__json_str(col, path) -%}
  {{ col }}:{{ json_path(path) }}::string
{%- endmacro %}

{% macro duckdb__json_num(col, path) -%}
  TRY_CAST({{ duckdb__json_str(col, path) }} AS DOUBLE)
{%- endmacro %}

{% macro snowflake__json_num(col, path) -%}
  TRY_CAST({{ snowflake__json_str(col, path) }} AS DOUBLE)
{%- endmacro %}

{% macro duckdb__json_ts(col, path) -%}
  TRY_CAST(SUBSTR({{ duckdb__json_str(col, path) }}, 1, 19) AS TIMESTAMP)
{%- endmacro %}

{% macro snowflake__json_ts(col, path) -%}
  TRY_CAST(SUBSTR({{ snowflake__json_str(col, path) }}, 1, 19) AS TIMESTAMP_NTZ)
{%- endmacro %}

{#
  One row per element of a JSON array, for arrays such as Encounter.location and Encounter.participant:

    FROM latest l {{ json_rows('l.raw', 'location', 'loc') }}      then json_str('loc.value', 'status')
                                                                    and json_row_index('loc') for the position

    DuckDB:    CROSS JOIN json_each(l.raw, '$.location') AS loc       (loc.key is the position as text)
    Snowflake: , LATERAL FLATTEN(input => l.raw:location) loc         (loc.index is the position)

  A resource without the array gives no rows, so use it in its own CTE and LEFT JOIN the result.
#}

{% macro json_rows(col, path, alias) -%}
  {%- set d = json_dialect() -%}
  {%- if d == 'snowflake' -%}{{ return(snowflake__json_rows(col, path, alias)) }}
  {%- elif d == 'duckdb' -%}{{ return(duckdb__json_rows(col, path, alias)) }}
  {%- endif -%}
  {{ return(adapter.dispatch('json_rows')(col, path, alias)) }}
{%- endmacro %}

{% macro json_row_index(alias) -%}
  {%- set d = json_dialect() -%}
  {%- if d == 'snowflake' -%}{{ return(snowflake__json_row_index(alias)) }}
  {%- elif d == 'duckdb' -%}{{ return(duckdb__json_row_index(alias)) }}
  {%- endif -%}
  {{ return(adapter.dispatch('json_row_index')(alias)) }}
{%- endmacro %}

{% macro duckdb__json_rows(col, path, alias) -%}
  CROSS JOIN json_each({{ col }}, '$.{{ json_path(path) }}') AS {{ alias }}
{%- endmacro %}

{% macro snowflake__json_rows(col, path, alias) -%}
  , LATERAL FLATTEN(input => {{ col }}:{{ json_path(path) }}) {{ alias }}
{%- endmacro %}

{% macro duckdb__json_row_index(alias) -%}
  CAST({{ alias }}.key AS INTEGER)
{%- endmacro %}

{% macro snowflake__json_row_index(alias) -%}
  {{ alias }}.index
{%- endmacro %}

{#
  The JSON document a FHIR Binary carries (Binary.data, base64), for the JSON documents this project publishes as
  Binaries (Binary/empi-review, Binary/empi-truth):

    DuckDB:    decode(from_base64(expr))                 JSON text, read with json_str and json_rows
    Snowflake: PARSE_JSON(BASE64_DECODE_STRING(expr))    a VARIANT
#}

{% macro json_from_base64(expr) -%}
  {%- set d = json_dialect() -%}
  {%- if d == 'snowflake' -%}{{ return(snowflake__json_from_base64(expr)) }}
  {%- elif d == 'duckdb' -%}{{ return(duckdb__json_from_base64(expr)) }}
  {%- endif -%}
  {{ return(adapter.dispatch('json_from_base64')(expr)) }}
{%- endmacro %}

{% macro duckdb__json_from_base64(expr) -%}
  decode(from_base64({{ expr }}))
{%- endmacro %}

{% macro snowflake__json_from_base64(expr) -%}
  PARSE_JSON(BASE64_DECODE_STRING({{ expr }}))
{%- endmacro %}
