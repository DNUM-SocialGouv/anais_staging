
  
  create view "duckdb_database"."main"."staging__v_region__dbt_tmp" as (
    

SELECT * FROM "duckdb_database"."main"."v_region"
  );
