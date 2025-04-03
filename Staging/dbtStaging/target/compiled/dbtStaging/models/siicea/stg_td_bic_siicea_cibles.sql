-- models/staging/siicea/stg_td_bic_siicea_cibles.sql
WITH source AS (
    SELECT 
        id_cible,
        code_finess,
        date_cible,
        type_cible,
        statut_cible,
        responsable_cible,
        objectif_cible,
        COUNT(*) OVER () AS total_cibles,
        RANK() OVER (PARTITION BY code_finess ORDER BY date_cible DESC) AS rang_cible
    FROM "duckdb_database"."main"."td_bic_siicea_cibles"
)
SELECT * FROM source