
  create view "staging"."public"."staging__sa_sirec__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."sa_sirec"
  );