{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('main', 'DC_det') }}