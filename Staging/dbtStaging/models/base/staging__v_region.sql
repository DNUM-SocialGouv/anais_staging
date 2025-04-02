{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('main', 'v_region') }}