{% macro replace_double_apostophes(colname) %}
    {# Remplace les doubles apostrophes par § #}
    {% set col_one_apostrophes = col_one_apostrophes | replace("''", "§") %}
    {{ return((col_one_apostrophes|string)) }}
{% endmacro %}

{% macro double_characters_for_accent(colname) %}
    {% set col_double_accents = colname|string %}
    
    {% set col_double_accents = col_double_accents
        | replace("à", "à¤")
        | replace("â", "â¤")
        | replace("ä", "ä¤")
        | replace("é", "é¤")
        | replace("è", "è¤")
        | replace("ê", "ê¤")
        | replace("ë", "ë¤")
        | replace("î", "î¤")
        | replace("ï", "ï¤")
        | replace("ô", "ô¤")
        | replace("ö", "ö¤")
        | replace("ù", "ù¤")
        | replace("û", "û¤")
        | replace("ü", "ü¤")
        | replace("ç", "ç¤") %}

    {{ return(col_double_accents) }}
{% endmacro %}

{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set col_str = colname|string %}
        {% set col_str = dbtStaging.replace_double_apostophes(col_str) %}
        {% set col_str = dbtStaging.double_characters_for_accent(col_str) %}
        {% set col_str = col_str[:adjusted_size] %}
        {% set col_str = col_str | replace("§", "''") | replace("¤", "") %}

        {{ return(col_str) }}
    {% else %}
        {{ return(colname|string).strip() }}
    {% endif %}
{% endmacro %}
