{% test expect_column_to_exist(model, column_name) %}

with actual_columns as (
    select column_name
    from information_schema.columns
    where lower(table_schema) = lower('{{ model.schema }}')
      and lower(table_name) = lower({{ model.name }})
)

select {{ column_name }} as missing_column
where not exists (
    select 1
    from actual_columns
    where lower(column_name) = lower({{ column_name }})
)

{% endtest %}
