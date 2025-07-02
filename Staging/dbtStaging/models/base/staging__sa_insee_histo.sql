{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('main', 'sa_insee_histo') }}