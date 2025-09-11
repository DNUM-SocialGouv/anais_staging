{% test expect_type_for_column(model, column_name, expected_type) %}
   {% set type_mapping = {
        'INTEGER': ('integer', 'bigint'),
        'VARCHAR': ('varchar', 'text', 'character varying'),
        'FLOAT':   ('float', 'double precision', 'real', 'numeric'),
        'DATE': ('date', 'date'),
        'BOOLEAN': ('bool', 'boolean')
    } %}
    {% set allowed_types = type_mapping[expected_type] %}

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
