{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source(get_source_schema(), 'cert_dc_insern') }}