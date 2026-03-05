{% macro cast_timestamp(column_name) %}
  {% if target.type == 'bigquery' %}
    SAFE_CAST({{ column_name }} AS TIMESTAMP)
  {% else %}
    {{ column_name }}::timestamp
  {% endif %}
{% endmacro %}

{% macro cast_date(column_name) %}
  {% if target.type == 'bigquery' %}
    SAFE_CAST({{ column_name }} AS DATE)
  {% else %}
    {{ column_name }}::date
  {% endif %}
{% endmacro %}

{% macro cast_integer(column_name) %}
  {% if target.type == 'bigquery' %}
    SAFE_CAST({{ column_name }} AS INT64)
  {% else %}
    {{ column_name }}::integer
  {% endif %}
{% endmacro %}

{% macro cast_numeric(column_name, precision=None, scale=None) %}
  {% if target.type == 'bigquery' %}
    SAFE_CAST({{ column_name }} AS NUMERIC)
  {% else %}
    {% if precision and scale %}
      {{ column_name }}::numeric({{ precision }}, {{ scale }})
    {% else %}
      {{ column_name }}::numeric
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro date_trunc(datepart, column_name) %}
  {% if target.type == 'bigquery' %}
    TIMESTAMP_TRUNC({{ column_name }}, {{ datepart.upper() }})
  {% else %}
    date_trunc('{{ datepart }}', {{ column_name }})
  {% endif %}
{% endmacro %}

{% macro date_diff(end_date, start_date, unit='day') %}
  {% if target.type == 'bigquery' %}
    DATE_DIFF({{ cast_date(end_date) }}, {{ cast_date(start_date) }}, {{ unit.upper() }})
  {% else %}
    ({{ end_date }}::date - {{ start_date }}::date)
  {% endif %}
{% endmacro %}

{% macro timestamp_diff_hours(end_timestamp, start_timestamp) %}
  {% if target.type == 'bigquery' %}
    TIMESTAMP_DIFF({{ end_timestamp }}, {{ start_timestamp }}, HOUR)
  {% else %}
    extract(epoch from ({{ end_timestamp }} - {{ start_timestamp }})) / 3600
  {% endif %}
{% endmacro %}
