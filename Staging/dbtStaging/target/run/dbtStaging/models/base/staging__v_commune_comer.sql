
  create view "staging"."public"."staging__v_commune_comer__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."v_commune_comer"
  );