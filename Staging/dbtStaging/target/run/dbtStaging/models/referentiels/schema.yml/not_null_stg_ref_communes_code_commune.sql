select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select code_commune
from "duckdb_database"."main"."stg_ref_communes"
where code_commune is null



      
    ) dbt_internal_test