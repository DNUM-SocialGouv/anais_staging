{{ config(
    materialized='view'
) }}

WITH missions AS (
    SELECT * FROM {{ ref('staging__sa_siicea_missions_real') }}
)

SELECT 
    COUNT(*) AS nombre_de_lignes,
    COUNT(DISTINCT identifiant_mission) AS nombre_distinct_id_missions,
    SUM(CASE WHEN LENGTH(cd_finess) = 8 THEN 1 ELSE 0 END) AS longueur_ko_finess,
    SUM(CASE WHEN LENGTH(cd_finess) = 9 THEN 1 ELSE 0 END) AS longueur_ok_finess,
    COUNT(DISTINCT LENGTH(cd_finess)) AS nombre_de_longueurs_differentes_finess,
    MIN(date_reelle_visite) AS date_reelle_visite_plus_ancienne,
    MAX(date_reelle_visite) AS date_reelle_visite_plus_recente,
    MIN(SUBSTRING(date_reelle_visite,1,4)) AS annee_min,
    MIN(SUBSTRING(date_reelle_visite,6,2)) AS mois_min,
    MAX(SUBSTRING(date_reelle_visite,1,4)) AS annee_max,
    MAX(SUBSTRING(date_reelle_visite,6,2)) AS mois_max
FROM missions