-- models/staging/siicea/stg_td_bic_siicea_missions_prog.sql
WITH source AS (
    SELECT 
        id_mission,
        code_finess,
        date_previsionnelle_mission,
        type_mission,
        statut_mission,
        responsable_mission,
        indicateur_prevision,
        COUNT(*) OVER () AS total_missions_planifiees,
        RANK() OVER (PARTITION BY code_finess ORDER BY date_previsionnelle_mission DESC) AS rang_mission_planifiee
    FROM "duckdb_database"."main"."td_bic_siicea_missions_prog"
)
SELECT * FROM source