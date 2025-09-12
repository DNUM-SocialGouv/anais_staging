{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set col_str = colname | string | lower %}
        {# Compte les doubles apostrophes #}
        {% set apostrophe_count = col_str.count("''") %}
        {% set accent_count = col_str.count("a") %}
        
        -- {# Liste des accents les plus fréquents #}
        -- {% set accents = ['a','â','ä','é','è','ê','ë','î','ï','ô','ö','ù','û','ü','ç'] %}
        
        -- {% set accent_count = 0 %}
        -- {% for a in accents %}
        --     {% if a in col_str %}
        --         {% set accent_count = accent_count + col_str.count(a) %}
        --     {% endif %}
        -- {% endfor %}
        
        {# Ajustement : +apostrophes, -accents #}
        {{ log("colname=" ~ col_str ~ " | apostrophes=" ~ apostrophe_count ~ " | accents=" ~ accent_count, info=True) }}
        {% set adjusted_size = size + apostrophe_count - accent_count %}
        
        {{ return(colname.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}
