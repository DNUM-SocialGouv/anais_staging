{{ config(
    materialized='view'
) }}

WITH regions AS (
    SELECT
        reg,
        ncc,
        libelle
    FROM {{ ref('staging__v_region') }}

    UNION

    SELECT
        comer AS reg,
        ncc,
        libelle
    FROM {{ ref('staging__v_comer') }} 
)

SELECT * FROM regions