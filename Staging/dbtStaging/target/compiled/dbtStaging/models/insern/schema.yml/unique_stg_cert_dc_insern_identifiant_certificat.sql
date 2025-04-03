
    
    

select
    identifiant_certificat as unique_field,
    count(*) as n_records

from "duckdb_database"."main"."stg_cert_dc_insern"
where identifiant_certificat is not null
group by identifiant_certificat
having count(*) > 1


