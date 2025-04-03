-- models/staging/siicea/stg_siicea_input_test.sql
WITH source AS (
    SELECT 
        COUNT(*) AS nombre_de_lignes,
        COUNT(DISTINCT identifiant_de_la_mission) AS nombre_distinct_id_missions,
        SUM(CASE WHEN LENGTH(code_finess) = 8 THEN 1 ELSE 0 END) AS longueur_ko_finess,
        SUM(CASE WHEN LENGTH(code_finess) = 9 THEN 1 ELSE 0 END) AS longueur_ok_finess,
        COUNT(DISTINCT LENGTH(code_finess)) AS nombre_de_longueurs_differentes_finess,
        MIN(date_reelle_visite) AS date_reelle_visite_plus_ancienne,
        MAX(date_reelle_visite) AS date_reelle_visite_plus_recente,
        MIN(SUBSTR(date_reelle_visite, 1, 4)) AS annee_min,
        MIN(SUBSTR(date_reelle_visite, 6, 2)) AS mois_min,
        MAX(SUBSTR(date_reelle_visite, 1, 4)) AS annee_max,
        MAX(SUBSTR(date_reelle_visite, 6, 2)) AS mois_max
    FROM "duckdb_database"."main"."sa_siicea_missions_20240930"
)
SELECT * FROM source