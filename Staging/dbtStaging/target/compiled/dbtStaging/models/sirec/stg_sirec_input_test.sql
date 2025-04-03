-- models/staging/sirec/stg_sirec_input_test.sql
WITH source AS (
    SELECT 
        COUNT(*) AS nombre_de_lignes,
        COUNT(DISTINCT numero_reclamation) AS nombre_distinct_reclamations,
        SUM(CASE WHEN LENGTH(code_client) = 6 THEN 1 ELSE 0 END) AS longueur_ko_code_client,
        SUM(CASE WHEN LENGTH(code_client) = 7 THEN 1 ELSE 0 END) AS longueur_ok_code_client,
        COUNT(DISTINCT LENGTH(code_client)) AS nombre_de_longueurs_differentes_code_client,
        MIN(date_reclamation) AS date_reclamation_plus_ancienne,
        MAX(date_reclamation) AS date_reclamation_plus_recente,
        MIN(SUBSTR(date_reclamation, 1, 4)) AS annee_min,
        MIN(SUBSTR(date_reclamation, 6, 2)) AS mois_min,
        MAX(SUBSTR(date_reclamation, 1, 4)) AS annee_max,
        MAX(SUBSTR(date_reclamation, 6, 2)) AS mois_max
    FROM "duckdb_database"."main"."sa_sirec_reclamations_20240930"
)
SELECT * FROM source