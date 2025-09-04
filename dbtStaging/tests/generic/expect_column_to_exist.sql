{% test expect_column_to_exist(model, column_name) %}

with actual_columns as (
    select lower(column_name) as column_name
    from information_schema.columns
    where lower(table_schema) = lower('{{ model.schema }}')
      and lower(table_name) = lower('{{ model.name }}')
)

select lower('{{ column_name }}') as missing_column
where lower('{{ column_name }}') not in (select lower(column_name) from actual_columns)

{% endtest %}
