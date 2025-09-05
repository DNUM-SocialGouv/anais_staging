{% macro convert_date_format(schema, expected_format='YYYY-MM-DD') %}
   {% set mapping = [
        ("YYYY", "%Y"),
        ('YY',   '%y'),
        ('MM',   '%m'),
        ('DD',   '%d'),
        ('HH24', '%H'),
        ('MI',   '%M'),
        ('SS',   '%S')
    ] %}

    {% set regex_mapping = [
        ('YYYY', '\\d{4}'),
        ('YY',   '\\d{2}'),
        ('MM',   '\\d{2}'),
        ('DD',   '\\d{2}'),
        ('HH24', '\\d{2}'),
        ('MI',   '\\d{2}'),
        ('SS',   '\\d{2}')
    ] %}
    {% set format_date = namespace(val=expected_format) %}
    {% set regex_pattern = namespace(val=expected_format) %}

    {% if schema == 'main' %}
        {% for k, v in mapping %}
            {% set format_date.val = format_date.val |replace(k, v) %}
        {% endfor %}
    {% endif %}
    
    {% for k, v in regex_mapping %}
        {% set regex_pattern.val = regex_pattern.val | replace(k, v) %}
    {% endfor %}

    {{ return({'format': format_date.val, 'regex': '^' ~ regex_pattern.val ~ '$'}) }}
{% endmacro %}
