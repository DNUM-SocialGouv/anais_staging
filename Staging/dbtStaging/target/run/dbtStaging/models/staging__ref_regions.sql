
  
  create view "duckdb_database"."main"."staging__ref_regions__dbt_tmp" as (
    

WITH regions AS (
    SELECT
        REG AS reg,
        NCC AS ncc,
        LIBELLE AS libelle
    FROM "duckdb_database"."main"."staging__v_region"

    UNION

    SELECT
        COMER AS reg,
        NCC AS ncc,
        LIBELLE AS libelle
    FROM "duckdb_database"."main"."staging__v_comer" 
)

SELECT * FROM regions
  );
