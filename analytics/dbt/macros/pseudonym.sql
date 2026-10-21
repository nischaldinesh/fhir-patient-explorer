{#
  Stable pseudonym for a patient id: the same formula in DuckDB and Snowflake, so with the same secret salt
  an analyst gets the same patient_key in both engines. The salt comes from the var pseudonym_salt (build.py
  passes it) or the PSEUDONYM_SALT environment variable, with a local demo value when neither is set.
#}
{% macro pseudonym_salt() -%}
  {{ return(var('pseudonym_salt', env_var('PSEUDONYM_SALT', '') or 'local-demo-salt')) }}
{%- endmacro %}

{% macro patient_key(col) -%}
  'P-' || UPPER(SUBSTR(MD5('{{ pseudonym_salt() | replace("'", "''") }}' || {{ col }}), 1, 10))
{%- endmacro %}
