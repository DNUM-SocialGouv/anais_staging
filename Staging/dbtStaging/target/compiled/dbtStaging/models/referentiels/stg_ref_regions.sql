-- models/staging/referentiels/stg_ref_regions.sql
WITH source AS (
    SELECT 
        code_region,
        nom_region,
        population_region,
        superficie_region,
        COUNT(*) OVER () AS total_regions
    FROM "duckdb_database"."main"."ref_regions"
)
SELECT * FROM source