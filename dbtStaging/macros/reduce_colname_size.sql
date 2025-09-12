{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set col_str = colname|string %}
        {# Compte les doubles apostrophes #}
        {% set apostrophe_count = col_str.count("''") %}
        
        {# Liste des accents les plus fréquents #}
        {% set accents = ["a", "à","â","ä","é","è","ê","ë","î","ï","ô","ö","ù","û","ü","ç"] %}
        
        {% set col_without_accents = colname|string %}
        {{ log("Avant replace: " ~ col_without_accents, info=True) }}
        
        {% set col_without_accents = col_without_accents | replace("à", "") %}
        {% set col_without_accents = col_without_accents | replace("â", "") %}
        {% set col_without_accents = col_without_accents | replace("ä", "") %}
        {% set col_without_accents = col_without_accents | replace("é", "") %}
        {% set col_without_accents = col_without_accents | replace("è", "") %}
        {% set col_without_accents = col_without_accents | replace("ê", "") %}
        {% set col_without_accents = col_without_accents | replace("ë", "") %}
        {% set col_without_accents = col_without_accents | replace("î", "") %}
        {% set col_without_accents = col_without_accents | replace("ï", "") %}
        {% set col_without_accents = col_without_accents | replace("ô", "") %}
        {% set col_without_accents = col_without_accents | replace("ö", "") %}
        {% set col_without_accents = col_without_accents | replace("ù", "") %}
        {% set col_without_accents = col_without_accents | replace("û", "") %}
        {% set col_without_accents = col_without_accents | replace("ü", "") %}
        {% set col_without_accents = col_without_accents | replace("ç", "") %}

        {{ log("Après replace: " ~ col_without_accents, info=True) }}
        {% set accent_count = (colname|length) - (col_without_accents|length) %}

        {# Log pour debug #}
        {{ log("colname=" ~ colname ~ " | apostrophes=" ~ apostrophe_count ~ " | accents=" ~ accent_count, info=True) }}

        {# Ajustement : +apostrophes, -accents #}
        {% set adjusted_size = size + apostrophe_count - accent_count %}
        
        {{ return(colname.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname|string).strip() }}
    {% endif %}
{% endmacro %}
