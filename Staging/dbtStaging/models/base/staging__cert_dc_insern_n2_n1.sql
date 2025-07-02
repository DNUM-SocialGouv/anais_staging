{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('main', 'cert_dc_insern_n2_n1') }}