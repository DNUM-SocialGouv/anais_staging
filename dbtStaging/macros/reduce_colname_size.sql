{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set apostrophe_count = colname.count("''") %}
        
        {# Compte les caractères non ASCII (accents, ç, etc.) #}
        {% set accent_count = 0 %}
        {% for c in colname %}
            {% if c|int(base=16, default=0) == 0 %}  {# fallback au cas où #}
            {% endif %}
        {% endfor %}
        {% for c in colname %}
            {% if c|ord > 127 %}
                {% set accent_count = accent_count + 1 %}
            {% endif %}
        {% endfor %}
        {{ log("colname=" ~ colname ~ " | apostrophes=" ~ apostrophe_count ~ " | accents=" ~ accent_count, info=True) }}
        {% set adjusted_size = size + apostrophe_count - accent_count %}
        
        {{ return(colname.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}

