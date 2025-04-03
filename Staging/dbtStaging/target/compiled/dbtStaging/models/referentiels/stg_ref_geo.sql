-- models/staging/referentiels/stg_ref_geo.sql
WITH source AS (
    SELECT 
        code_geo,
        nom_geo,
        type_geo,
        code_parent_geo,
        COUNT(*) OVER () AS total_geo
    FROM "duckdb_database"."main"."ref_geo"
)
SELECT * FROM source