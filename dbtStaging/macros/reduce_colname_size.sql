{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set col_str = colname|string %}
        {# Compte les doubles apostrophes #}
        {% set apostrophe_count = col_str.count("''") %}
        
        {# Liste des accents les plus fréquents #}
        {% set accents = ['a','â','ä','é','è','ê','ë','î','ï','ô','ö','ù','û','ü','ç'] %}
        
        {% set col_without_accents = col_str %}
        {% for a in accents %}
            {% set col_without_accents = col_without_accents | replace(a, "") %}
        {% endfor %}
        
        {% set accent_count = col_str|length - col_without_accents|length %}
        {{ log("Avant replace: " ~ col_without_accents, info=True) }}
        {% set col_without_accents = col_without_accents | replace("a", "") %}
        {{ log("Après replace: " ~ col_without_accents, info=True) }}

        {# Log pour debug #}
        {{ log("colname=" ~ col_str ~ " | apostrophes=" ~ apostrophe_count ~ " | accents=" ~ accent_count, info=True) }}

        {# Ajustement : +apostrophes, -accents #}
        {% set adjusted_size = size + apostrophe_count - accent_count %}
        
        {{ return(col_str.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname|string).strip() }}
    {% endif %}
{% endmacro %}
