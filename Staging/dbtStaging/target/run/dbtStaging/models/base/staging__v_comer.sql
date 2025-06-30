
  create view "staging"."public"."staging__v_comer__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."v_comer"
  );