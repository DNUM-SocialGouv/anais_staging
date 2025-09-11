{% test expect_type_for_column(model, column_name) %}
   {% set type_mapping = {
        'INTEGER': ('integer', 'bigint'),
        'VARCHAR': ('varchar', 'text', 'character varying'),
        'FLOAT':   ('float', 'double precision', 'real', 'numeric'),
        'DATE': ('date', 'date'),
        'BOOLEAN': ('bool', 'boolean')
    } %}
    {% set expected_type = column.get('data_type') %}

    {% if not expected_type %}
        {% do exceptions.raise_compiler_error(
            "Le champ 'data_type' est requis dans le schema.yml pour la colonne '" ~ column_name ~ "'"
        ) %}
    {% endif %}

    with actual_columns as (
        select
            column_name,
            lower(data_type) as data_type
        from information_schema.columns
        where lower(table_schema) = lower('{{ model.schema }}')
        and lower(table_name) = lower('{{ model.name }}')
    )

    select
        '{{ column_name }}' as column_name,
        data_type as actual_type,
        '{{ expected_type }}' as expected_type,
        'La colonne {{ column_name }} a un type différent de {{ expected_type }} (trouvé: ' || data_type || ')' as message
    from actual_columns
    where column_name = '{{ column_name }}'
        and data_type not in {{ type_mapping[expected_type] }}

{% endtest %}
