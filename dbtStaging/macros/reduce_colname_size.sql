{% macro reduce_colname_size(schema_name, colname, size=63) %}
    {% if schema_name == 'public' %}
        {% set apostrophe_count = colname.count("''") %}
        
        {% set accents = ['à','â','ä','é','è','ê','ë','î','ï','ô','ö','ù','û','ü','ç'] %}
        
        
        {% set colname_without_accent = colname}
        {% for a in accents %}
            {{ colname_without_accent = colname_without_accent | replace(a,"")}}
        {% endfor %}
        
        {% set accent_count = (colname | length) - (colname_without_accent | length) %}
        {% set adjusted_size = size + apostrophe_count - accent_count %}
        
        {{ return(colname.strip()[:adjusted_size]) }}
    {% else %}
        {{ return(colname.strip()) }}
    {% endif %}
{% endmacro %}
