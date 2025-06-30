
  create view "staging"."public"."staging__v_departement__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."v_departement"
  );