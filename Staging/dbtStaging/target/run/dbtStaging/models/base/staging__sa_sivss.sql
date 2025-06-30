
  create view "staging"."public"."staging__sa_sivss__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."sa_sivss"
  );