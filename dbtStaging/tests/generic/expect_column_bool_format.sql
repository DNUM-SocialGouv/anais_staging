{% test expect_column_bool_format(model, column_name) %}
  {% set column_expr = dbtStaging.reduce_colname_size(model.schema, column_name) %}

  select *
    from {{ model }}
    where "{{ column_expr }}" is not null
      and "{{ column_expr }}" not in (true, false)

{% endtest %}
