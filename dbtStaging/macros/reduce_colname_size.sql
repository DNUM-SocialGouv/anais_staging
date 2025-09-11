{% macro reduce_colname_size(schema, colname, size=50) %}
    {% if schema == 'main' %}
        {{ return(colname[:size].strip()) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}