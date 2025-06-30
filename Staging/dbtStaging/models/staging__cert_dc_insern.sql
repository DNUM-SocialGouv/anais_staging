{{ config(
    materialized='view'
) }}

WITH insern AS (
    SELECT * FROM {{ ref('staging__sa_insern') }}
)

SELECT 
    ROW_NUMBER() OVER () AS index,
    CAST(Annee || '-' || LPAD(Mois::VARCHAR, 2, '0') || '-' || LPAD(Jour::VARCHAR, 2, '0') AS DATE) AS date_deces,
    Annee AS Annee,
    Mois AS Mois,
    Jour AS Jour,
    departement_code,
    commune_code,
    et_finess,
    Lieu_de_deces,
    Source,
    1 AS deces_nb
FROM insern