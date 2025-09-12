{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
{% set col_str = colname|string %}
{% set apostrophe_count = col_str.count("''") %}
{% set accent_count = col_str.count('a') %}        
        {% set adjusted_size = size + apostrophe_count - accent_count %}
        {% set col_str = colname|string %}
{% set apostrophe_count = col_str.count("''") %}
{% set accent_count = col_str.count('a') %}

        {{ return(colname.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}
