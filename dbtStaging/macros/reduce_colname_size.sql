{% macro reduce_colname_size(colname, size=50) %}
    {{ colname[:size] | trim }}
{% endmacro %}