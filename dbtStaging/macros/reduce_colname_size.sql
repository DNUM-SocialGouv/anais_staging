{% macro count_double_apostophes(colname) %}
    {# Compte les doubles apostrophes #}
    {{ (colname|string).count("''") }}
{% endmacro %}

{% macro count_accent_characters(colname) %}
    {% set col_without_accents = colname|string %}
    
    {% set col_without_accents = col_without_accents
        | replace("à", "")
        | replace("â", "")
        | replace("ä", "")
        | replace("é", "")
        | replace("è", "")
        | replace("ê", "")
        | replace("ë", "")
        | replace("î", "")
        | replace("ï", "")
        | replace("ô", "")
        | replace("ö", "")
        | replace("ù", "")
        | replace("û", "")
        | replace("ü", "")
        | replace("ç", "") %}

    {% set accent_count = (colname|length) - (col_without_accents|length) %}
    {{ accent_count }}
{% endmacro %}

{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set col_str = colname|string %}

        {# Ajustement : +apostrophes, -accents #}
        {% set apostrophe_count = count_double_apostophes(col_str[:size]) | int %}
        {% set adjusted_size = size + apostrophe_count %}
        {% set accent_count = count_accent_characters(col_str[:adjusted_size]) | int %}
        {% set adjusted_size = size - accent_count %}

        {# Log pour debug #}
        {{ log("colname=" ~ colname ~ " | apostrophes=" ~ apostrophe_count ~ " | accents=" ~ accent_count, info=True) }}

        {{ colname.strip()[:adjusted_size] }}
    {% else %}
        {{ (colname|string).strip() }}
    {% endif %}
{% endmacro %}
