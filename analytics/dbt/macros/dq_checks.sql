{#
  on-run-end hook: after `dbt build` or `dbt test`, save the test results in the warehouse.

    dq_test_results  every test: name, model, status, failures
    dq_checks        the named data quality checks, also published in the analyst snapshot (check_name, failures),
                     in page order. A test joins this list with config meta {check_name, check_order}.

  Nothing is written when the run had no tests (dbt seed, dbt run), so the last results stay in place.
#}
{% macro save_dq_checks(results) %}
  {% if not execute or flags.WHICH not in ('build', 'test') %}{{ return('') }}{% endif %}
  {% set tests = [] %}
  {% for r in results if r.node.resource_type == 'test' %}
    {% do tests.append(r) %}
  {% endfor %}
  {% if not tests %}{{ return('') }}{% endif %}

  {% set all_rows = [] %}
  {% set checks = [] %}
  {% for r in tests %}
    {% set meta = r.node.config.meta or {} %}
    {% set model = (r.node.attached_node or '').split('.')[-1] %}
    {% set failures = r.failures if r.failures is not none else 'NULL' %}
    {% do all_rows.append("(" ~ dq_literal(r.node.name) ~ ", " ~ dq_literal(model) ~ ", " ~ dq_literal(r.status) ~ ", "
                          ~ "CAST(" ~ failures ~ " AS BIGINT), " ~ dq_literal(meta.get('check_name')) ~ ")") %}
    {% if meta.get('check_name') %}
      {% do checks.append((meta.get('check_order', 999), meta.get('check_name'), failures)) %}
    {% endif %}
  {% endfor %}

  {% set sql %}
    CREATE OR REPLACE TABLE {{ target.schema }}.dq_test_results AS
    SELECT * FROM (VALUES {{ all_rows | join(', ') }}) AS t(test_name, model, status, failures, check_name);
  {% endset %}
  {% do run_query(sql) %}
  {% if checks %}
    {% set rows = [] %}
    {% for order, name, failures in checks | sort %}
      {% do rows.append("(" ~ dq_literal(name) ~ ", CAST(" ~ failures ~ " AS BIGINT))") %}
    {% endfor %}
    {% set sql %}
      CREATE OR REPLACE TABLE {{ target.schema }}.dq_checks AS
      SELECT * FROM (VALUES {{ rows | join(', ') }}) AS t(check_name, failures);
    {% endset %}
    {% do run_query(sql) %}
  {% endif %}
  {{ return('') }}
{% endmacro %}

{% macro dq_literal(value) -%}
  {%- if value is none -%}CAST(NULL AS VARCHAR){%- else -%}'{{ value | string | replace("'", "''") }}'{%- endif -%}
{%- endmacro %}
