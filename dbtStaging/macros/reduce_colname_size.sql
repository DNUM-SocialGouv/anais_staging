{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {# Compte les doubles apostrophes #}
        {% set apostrophe_count = colname.count("''") %}
        {% set accent_count = colname.count("a") %}
        
        {% set accents = ["a","â","ä","é","è","ê","ë","î","ï","ô","ö","ù","û","ü","ç"] %}
        
        -- {% set accent_count = 0 %}
        -- {% for a in accents %}
        --     {% if a in colname %}
        --         {% set accent_count = accent_count + colname.count(a) %}
        --     {% endif %}
        -- {% endfor %}
        
        {% set adjusted_size = size + apostrophe_count - accent_count %}
        {{ log("colname=" ~ colname ~ " | apostrophes=" ~ apostrophe_count ~ " | accents=" ~ accent_count, info=True) }}

        {{ return(colname.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}
