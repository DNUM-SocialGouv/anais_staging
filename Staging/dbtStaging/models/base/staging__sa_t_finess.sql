{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('main', 'sa_t_finess') }}