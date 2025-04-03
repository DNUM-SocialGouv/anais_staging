-- models/staging/referentiels/stg_ref_departements.sql
WITH source AS (
    SELECT 
        code_departement,
        nom_departement,
        code_region,
        population_departement,
        superficie_departement,
        COUNT(*) OVER () AS total_departements
    FROM "duckdb_database"."main"."ref_departements"
)
SELECT * FROM source