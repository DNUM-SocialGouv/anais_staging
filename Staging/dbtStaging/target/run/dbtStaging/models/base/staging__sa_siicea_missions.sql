
  create view "staging"."public"."staging__sa_siicea_missions__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."sa_siicea_missions"
  );