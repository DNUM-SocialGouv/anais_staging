{% macro reduce_colname_size(colname, size = 50) %}
    {{ return("substr(" ~ colname ~ ", 1, " ~ size ~ ")") }}
{% endmacro %}