{{ config(
    materialized='view'
) }}

WITH sirec AS (
    SELECT 
        "Numéro de la réclamation" AS numero_reclamation,
        "Date de réception à l’ARS" AS date_reception_ars,
        CAST("N° FINESS/RPPS" AS VARCHAR) AS numero_finess_rpps
    FROM {{ ref('staging__sa_sirec') }}
)

SELECT 
    COUNT(*) AS nombre_de_lignes,
    COUNT(DISTINCT numero_reclamation) AS nombre_distinct_reclamations,
    SUM(CASE WHEN LENGTH(numero_finess_rpps) = 8 THEN 1 ELSE 0 END) AS longueur_ko_finess,
    SUM(CASE WHEN LENGTH(numero_finess_rpps) = 9 THEN 1 ELSE 0 END) AS longueur_ok_finess,
    MIN(date_reception_ars) AS date_reelle_visite_plus_ancienne,
    MAX(date_reception_ars) AS date_reelle_visite_plus_recente,
    MIN(SUBSTRING(date_reception_ars, 1, 4)) AS annee_min,
    MIN(SUBSTRING(date_reception_ars, 6, 2)) AS mois_min,
    MAX(SUBSTRING(date_reception_ars, 1, 4)) AS annee_max,
    MAX(SUBSTRING(date_reception_ars, 6, 2)) AS mois_max
FROM sirec