select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select nombre_distinct_reclamations
from "duckdb_database"."main"."stg_sirec_input_test"
where nombre_distinct_reclamations is null



      
    ) dbt_internal_test