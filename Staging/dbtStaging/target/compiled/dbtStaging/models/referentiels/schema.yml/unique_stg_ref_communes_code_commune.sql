
    
    

select
    code_commune as unique_field,
    count(*) as n_records

from "duckdb_database"."main"."stg_ref_communes"
where code_commune is not null
group by code_commune
having count(*) > 1


