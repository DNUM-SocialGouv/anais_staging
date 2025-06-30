
  create view "staging"."public"."staging__v_region__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."v_region"
  );