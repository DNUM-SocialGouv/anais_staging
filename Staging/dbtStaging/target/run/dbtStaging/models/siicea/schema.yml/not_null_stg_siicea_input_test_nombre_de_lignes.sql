select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select nombre_de_lignes
from "duckdb_database"."main"."stg_siicea_input_test"
where nombre_de_lignes is null



      
    ) dbt_internal_test