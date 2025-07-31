{{ config(
    materialized='view'
) }}

WITH insern AS (
    SELECT * FROM {{ ref('staging__sa_insern') }}
)

SELECT 
    CAST(ROW_NUMBER() OVER () AS VARCHAR) AS index,
    CAST(annee || '-' || LPAD(mois::VARCHAR, 2, '0') || '-' || LPAD(jour::VARCHAR, 2, '0') AS DATE) AS date_deces,
    annee,
    mois,
    jour,
    departement_code,
    commune_code,
    et_finess,
    lieu_de_deces,
    source,
    1 AS deces_nb
FROM insern