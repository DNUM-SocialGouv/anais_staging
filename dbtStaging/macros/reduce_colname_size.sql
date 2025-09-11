{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set apostrophe_count = colname.count("''") %}
        {% set adjusted_size = size + apostrophe_count %}
        {{ return(colname[:adjusted_size].strip()) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}