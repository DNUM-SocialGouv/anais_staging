{% macro reduce_colname_size(schema_name, colname, size=62) %}
    {% if schema_name == 'public' %}
        {{ return(colname[:size].strip()) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}