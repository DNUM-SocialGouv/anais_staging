
  
  create view "duckdb_database"."main"."staging__v_commune_comer__dbt_tmp" as (
    

SELECT * FROM "duckdb_database"."main"."v_commune_comer"
  );
