{{ config(
    materialized='view'
) }}

WITH sirec AS (
    SELECT * FROM {{ ref('staging__sa_sirec') }}
    WHERE date_de_reception_a_l_ars BETWEEN '2022-01-01' AND '2025-08-01'
)

SELECT * FROM sirec