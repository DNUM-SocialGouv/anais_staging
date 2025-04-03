-- models/staging/referentiels/stg_ref_communes.sql
WITH source AS (
    SELECT 
        code_commune,
        nom_commune,
        code_departement,
        population_commune,
        superficie_commune,
        COUNT(*) OVER () AS total_communes
    FROM "duckdb_database"."main"."ref_communes"
)
SELECT * FROM source