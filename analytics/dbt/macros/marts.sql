{#
  Keys for the star schema (models/marts/), written once in SQL both engines run unchanged (MD5, EXTRACT, CAST).

    surrogate_key(['a', 'b'])         MD5 of the parts joined with '|' (NULL parts as empty text): 32 hex characters.
                                      For master data whose ids say nothing about a patient (locations, staff, codes).
    salted_key(['a', 'b'])            the same with the pseudonym salt in front, for every key that stands for a
                                      patient or one of their records (person, record, visit, diagnosis, result). The
                                      FHIR ids of the live feed carry the MRN (riverside-rv200003), so these keys must
                                      not be reversible by anyone who knows the ids. A new salt gives new keys: rebuild
                                      with dbt build --full-refresh.
    date_key(d)                       YYYYMMDD as an integer (20261014), NULL for a NULL date.
    month_start_key(d)                the date key of the first day of d's month (20261001).
    unknown_key()                     the key of the "Unknown" member every dimension carries, so a fact never needs
                                      a NULL foreign key for a dimension that has one.
#}

{% macro surrogate_key(parts) -%}
  MD5({% for p in parts %}COALESCE(CAST({{ p }} AS VARCHAR), ''){% if not loop.last %} || '|' || {% endif %}{% endfor %})
{%- endmacro %}

{% macro salted_key(parts) -%}
  MD5({{ salt_expression() }} || '|' || {% for p in parts %}COALESCE(CAST({{ p }} AS VARCHAR), ''){% if not loop.last %} || '|' || {% endif %}{% endfor %})
{%- endmacro %}

{#
  The salt in SQL. The rendered Snowflake files never carry it (they are committed): they read it from
  governance_settings, which sql/snowflake/00_setup_and_load.sql creates.
#}
{% macro salt_expression() -%}
  {%- if json_dialect() == 'snowflake' -%}
    (SELECT pseudonym_salt FROM governance_settings)
  {%- else -%}
    '{{ pseudonym_salt() | replace("'", "''") }}'
  {%- endif -%}
{%- endmacro %}

{% macro date_key(d) -%}
  CAST(EXTRACT(YEAR FROM {{ d }}) * 10000 + EXTRACT(MONTH FROM {{ d }}) * 100 + EXTRACT(DAY FROM {{ d }}) AS INTEGER)
{%- endmacro %}

{% macro month_start_key(d) -%}
  CAST(EXTRACT(YEAR FROM {{ d }}) * 10000 + EXTRACT(MONTH FROM {{ d }}) * 100 + 1 AS INTEGER)
{%- endmacro %}

{% macro unknown_key() -%}
  'unknown'
{%- endmacro %}

{#
  Small cell suppression (CMS cell size rule: no cell of 1 to 10 people or events is shown). True when the
  denominator is 1 to 10, or, for a count numerator, the numerator or what is left of the denominator is 1 to 10
  (otherwise the rate and the denominator would give the small count back). 0 is not a small cell.
#}
{% macro is_small_cell(numerator, denominator, numerator_is_count) -%}
  (({{ denominator }} BETWEEN 1 AND 10)
   {%- if numerator_is_count %}
   OR ({{ numerator }} BETWEEN 1 AND 10) OR ({{ denominator }} - {{ numerator }} BETWEEN 1 AND 10)
   {%- endif %})
{%- endmacro %}

{#
  The same rule for SQL values rather than a Jinja flag: true when the denominator d is 1 to 10, or, when count_flag
  (a SQL boolean) is true, the numerator n or the complement c is 1 to 10. mart_quality_period uses it for a cell's own
  counts (c = d - n) and for the residual a reader could get by subtracting the shown cells inside it.
#}
{% macro small_counts(n, d, c, count_flag) -%}
  (({{ d }} BETWEEN 1 AND 10) OR ({{ count_flag }} AND (({{ n }} BETWEEN 1 AND 10) OR ({{ c }} BETWEEN 1 AND 10))))
{%- endmacro %}

{#
  cover_of(rel): the rows of rel (cells with small_cell, small_residual and their children's cover kn, kd, kc) with the
  cover a reader gets from them: the cell's own counts when it is shown, else what its children cover (fn, fd, fc).
#}
{% macro cover_of(rel) -%}
    SELECT r.*,
           CASE WHEN r.small_cell OR r.small_residual THEN r.kn ELSE r.n END AS fn,
           CASE WHEN r.small_cell OR r.small_residual THEN r.kd ELSE r.d END AS fd,
           CASE WHEN r.small_cell OR r.small_residual THEN r.kc ELSE r.c END AS fc
    FROM {{ rel }} r
{%- endmacro %}
