-- models/staging/siicea/stg_td_bic_siicea_missions_real.sql
WITH source AS (
    SELECT 
        id_mission,
        code_finess,
        date_mission,
        type_mission,
        statut_mission,
        responsable_mission,
        indicateur_realisation,
        COUNT(*) OVER () AS total_missions_realisees,
        RANK() OVER (PARTITION BY code_finess ORDER BY date_mission DESC) AS rang_mission_realisee
    FROM "duckdb_database"."main"."td_bic_siicea_missions_real"
)
SELECT * FROM source