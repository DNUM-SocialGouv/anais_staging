select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select nombre_distinct_id_missions
from "duckdb_database"."main"."stg_siicea_input_test"
where nombre_distinct_id_missions is null



      
    ) dbt_internal_test