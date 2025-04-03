
  
  create view "duckdb_database"."main"."staging__v_commune_depuis__dbt_tmp" as (
    

SELECT * FROM "duckdb_database"."main"."v_commune_depuis"
  );
