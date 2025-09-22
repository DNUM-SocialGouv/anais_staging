{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source(dbtStaging.get_source_schema(), 'cert_dc_insern_2023_2024') }}