{% test expect_column_bool_format(model, column_name) %}

select *
from {{ model }}
where "{{ column_name }}" is not null
  and "{{ column_name }}" not in (true, false)

{% endtest %}
