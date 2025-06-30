
  create view "staging"."public"."staging__sa_t_finess__dbt_tmp"
    
    
  as (
    

SELECT * FROM "staging"."public"."sa_t_finess"
  );