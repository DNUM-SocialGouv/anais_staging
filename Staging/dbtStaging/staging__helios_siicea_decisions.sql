{{ config(
    materialized='view'
) }}

SELECT * FROM {{ ref('staging__sa_siicea_decisions') }};