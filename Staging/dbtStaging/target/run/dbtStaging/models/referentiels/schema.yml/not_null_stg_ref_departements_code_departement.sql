select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select code_departement
from "duckdb_database"."main"."stg_ref_departements"
where code_departement is null



      
    ) dbt_internal_test