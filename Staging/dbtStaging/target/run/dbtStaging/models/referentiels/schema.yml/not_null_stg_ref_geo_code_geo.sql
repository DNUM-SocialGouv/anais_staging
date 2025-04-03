select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select code_geo
from "duckdb_database"."main"."stg_ref_geo"
where code_geo is null



      
    ) dbt_internal_test