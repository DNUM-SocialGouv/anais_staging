{{ config(
    materialized='view'
) }}

WITH sirec AS (
    SELECT * FROM {{ ref('staging__sa_sirec') }}
    WHERE "Date de réception à l’ARS" BETWEEN '2022-01-01' AND '2024-12-31'
)

SELECT * FROM sirec