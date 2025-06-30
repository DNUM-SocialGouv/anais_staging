
  create view "staging"."public"."staging__v_commune_depuis__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."v_commune_depuis"
  );