select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select code_region
from "duckdb_database"."main"."stg_ref_regions"
where code_region is null



      
    ) dbt_internal_test