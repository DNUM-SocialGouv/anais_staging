select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select identifiant_certificat
from "duckdb_database"."main"."stg_cert_dc_tfiness"
where identifiant_certificat is null



      
    ) dbt_internal_test