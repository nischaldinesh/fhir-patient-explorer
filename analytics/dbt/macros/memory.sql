{#- DuckDB settings from the environment, only when the variable is set: duckdb_setting('memory_limit',
    'DUCKDB_MEMORY_LIMIT') and duckdb_setting('threads', 'DUCKDB_THREADS'), run as on-run-start hooks. Local Airflow
    sets them so dbt stays inside its container's memory limit; elsewhere they are unset and DuckDB keeps its defaults
    (80 percent of RAM, one thread per core). The memory limit must be an absolute size such as 1500MB. -#}
{% macro duckdb_setting(name, env) %}
  {%- set value = env_var(env, '') | trim -%}
  {%- if value and target.type == 'duckdb' -%}
    SET {{ name }} = '{{ value }}'
  {%- endif -%}
{% endmacro %}
