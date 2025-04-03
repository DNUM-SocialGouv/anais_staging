-- models/staging/sirec/stg_matrice_sirec.sql
WITH source AS (
    SELECT 
        id_reclamation,
        code_client,
        date_reclamation,
        type_reclamation,
        statut_reclamation,
        COUNT(*) OVER () AS total_reclamations,
        RANK() OVER (PARTITION BY code_client ORDER BY date_reclamation DESC) AS rang_reclamation
    FROM "duckdb_database"."main"."matrice_sirec"
)
SELECT * FROM source