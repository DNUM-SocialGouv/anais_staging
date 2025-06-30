{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source(get_source_schema(), 'v_departement') }}