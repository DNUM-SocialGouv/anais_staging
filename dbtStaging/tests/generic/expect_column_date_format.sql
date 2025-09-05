{% test expect_column_date_format(model, column_name, expected_format='YYYY-MM-DD') %}
{% set format = dbtStaging.convert_date_format(model.schema, expected_format) %}
{% set fmt_pattern = format['regex'] %}
{% set fmt_format = format['format'] %}

with column_info as (
    select
        lower(data_type) as data_type
    from information_schema.columns
    where lower(table_schema) = lower('{{ model.schema }}')
      and lower(table_name) = lower('{{ model.name }}')
      and column_name = '{{ column_name }}'
),
formatted_values as (
    select
        '{{ column_name }}' as raw_value,
        case
            when lower(data_type) in ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone')
            {% if model.schema == 'main' %}
                then strftime("{{ column_name }}", '{{ fmt_format }}')
            {% else %}
                then to_char("{{ column_name }}", '{{ fmt_format }}')
            {% endif %}
            else cast("{{ column_name }}" as varchar)
        end as formatted_value
    from {{ model }}, column_info
)

select *
from formatted_values
where formatted_value !~ '{{ fmt_pattern }}'  -- regex pour YYYY-MM-DD
  and raw_value is not null
{% endtest %}
