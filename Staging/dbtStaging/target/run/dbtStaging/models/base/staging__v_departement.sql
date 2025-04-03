
  
  create view "duckdb_database"."main"."staging__v_departement__dbt_tmp" as (
    

SELECT * FROM "duckdb_database"."main"."v_departement"
  );
