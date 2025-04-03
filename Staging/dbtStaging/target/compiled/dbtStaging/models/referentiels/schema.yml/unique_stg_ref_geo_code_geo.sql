
    
    

select
    code_geo as unique_field,
    count(*) as n_records

from "duckdb_database"."main"."stg_ref_geo"
where code_geo is not null
group by code_geo
having count(*) > 1


