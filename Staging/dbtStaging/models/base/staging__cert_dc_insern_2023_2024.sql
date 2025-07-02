{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('main', 'cert_dc_insern_2023_2024') }}