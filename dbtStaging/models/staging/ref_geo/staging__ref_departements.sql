{{ config(
    materialized='view'
) }}

WITH departements AS (
    -- Départements métropolitains
    SELECT
        dep,
        reg,
        ncc,
        libelle
    FROM {{ ref('staging__v_departement') }} 

    UNION

    -- Départements des DROM via v_comer
    SELECT
        comer AS dep,
        comer AS reg,
        ncc,
        libelle
    FROM {{ ref('staging__v_comer') }}
)

SELECT * FROM departements