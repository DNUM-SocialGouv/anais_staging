{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set apostrophe_count = colname.count("''") %}
        
        {% set accents = ["à","â","ä","é","è","ê","ë","î","ï","ô","ö","ù","û","ü","ç"] %}
        
        {% set accent_count = 0 %}
        {% set colname_without_accent = colname | regex_replace("[^A-Za-z0-9]", "") %}
        -- {% for a in accents %}
        --     {% set accent_count = accent_count + colname.count(a) %}
        {% set accent_count = (colname | length) - (colname_without_accent | length) %}
        {% set adjusted_size = size + apostrophe_count - accent_count %}
        
        {{ return(colname.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}
