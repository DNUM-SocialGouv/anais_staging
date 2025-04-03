-- models/staging/siicea/stg_helios_siicea_missions.sql
WITH source AS (
    SELECT 
        id_mission,
        code_finess,
        date_mission,
        type_mission,
        statut_mission,
        responsable_mission,
        COUNT(*) OVER () AS total_missions,
        RANK() OVER (PARTITION BY code_finess ORDER BY date_mission DESC) AS rang_mission
    FROM "duckdb_database"."main"."helios_siicea_missions"
)
SELECT * FROM source