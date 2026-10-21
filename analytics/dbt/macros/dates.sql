{#
  Calendar helpers written once in SQL both engines run unchanged (DATEDIFF, MOD, FLOOR, LEAST, GREATEST and
  DATE + integer), so no dispatch is needed. The argument is a DATE expression.

    weekday_index(d)        0 Monday ... 6 Sunday (counted from Monday 2024-01-01)
    week_start(d)           the Monday of d's ISO week
    add_business_days(d, n) n business days after d (Monday to Friday; public holidays are not known here).
                            A Friday plus 2 is the next Tuesday; a weekend day first moves back to its Friday.
#}

{% macro weekday_index(d) -%}
  MOD(MOD(DATEDIFF('day', DATE '2024-01-01', {{ d }}), 7) + 7, 7)
{%- endmacro %}

{% macro week_start(d) -%}
  ({{ d }} - CAST({{ weekday_index(d) }} AS INTEGER))
{%- endmacro %}

{% macro add_business_days(d, n) -%}
  ({{ d }} - CAST(GREATEST({{ weekday_index(d) }} - 4, 0) AS INTEGER)
       + CAST({{ n }} + 2 * FLOOR((LEAST({{ weekday_index(d) }}, 4) + {{ n }}) / 5) AS INTEGER))
{%- endmacro %}
