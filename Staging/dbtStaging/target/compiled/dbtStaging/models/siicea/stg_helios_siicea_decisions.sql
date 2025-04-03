-- models/staging/siicea/stg_helios_siicea_decisions.sql
WITH source AS (
    SELECT 
        id_decision,
        id_mission,
        code_finess,
        date_decision,
        type_decision,
        statut_decision,
        responsable_decision,
        COUNT(*) OVER () AS total_decisions,
        RANK() OVER (PARTITION BY id_mission ORDER BY date_decision DESC) AS rang_decision
    FROM "duckdb_database"."main"."helios_siicea_decisions"
)
SELECT * FROM source