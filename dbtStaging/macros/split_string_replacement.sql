{% macro split_string_by_pipe(column, table_alias='b') %}
    {% set schema = dbtStaging.get_source_schema() %}

    {% if schema == 'main' %}
        unnest(string_split({{ table_alias }}.{{ column }}, '|')) as motif(motif_value)
    {% elif schema == 'public' %}
        LATERAL (
            SELECT trim(value) AS motifs_igas_split
            FROM regexp_split_to_table(b.motifs_igas, '\|') AS value
        ) AS motif(motif_value)
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported schema for splitting string: " ~ schema) }}
    {% endif %}
{% endmacro %}