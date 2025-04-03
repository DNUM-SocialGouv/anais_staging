-- models/staging/sirec/stg_helios_sivss.sql
WITH source AS (
    SELECT 
        id_sivss,
        code_client,
        date_sivss,
        type_sivss,
        statut_sivss,
        responsable_sivss,
        COUNT(*) OVER () AS total_sivss,
        RANK() OVER (PARTITION BY code_client ORDER BY date_sivss DESC) AS rang_sivss
    FROM "duckdb_database"."main"."helios_sivss"
)
SELECT * FROM source