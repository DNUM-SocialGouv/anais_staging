
  create view "staging"."public"."staging__helios_sirec__dbt_tmp"
    
    
  as (
    

WITH sirec AS (
    SELECT * FROM "staging"."public"."staging__sa_sirec"
    WHERE "Date de réception à l’ARS" BETWEEN '2022-01-01' AND '2024-12-31'
)

SELECT * FROM sirec
  );