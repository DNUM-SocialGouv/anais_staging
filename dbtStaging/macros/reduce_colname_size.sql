{% macro reduce_colname_size(colname, size=50) %}
    {{ return(colname[:size].strip()) }}
{% endmacro %}