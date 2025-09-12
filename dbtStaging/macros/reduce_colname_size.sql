{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set apostrophe_count = colname.count("''") %}

        {# Compter les caractères accentués (non ASCII) #}
        {% set accent_count = 0 %}
        {% for c in colname %}
            {% if c.encode('utf-8')|length > 1 %}
                {% set accent_count = accent_count + 1 %}
            {% endif %}
        {% endfor %}

        {% set adjusted_size = size + apostrophe_count - accent_count %}

        {{ return(colname.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}
