-- models/staging/insern/stg_cert_dc_insern.sql
WITH source AS (
    SELECT 
        identifiant_certificat,
        code_insern,
        date_certificat,
        statut_certificat,
        COUNT(*) OVER () AS total_certificats,
        RANK() OVER (PARTITION BY code_insern ORDER BY date_certificat DESC) AS rang_certificat
    FROM "duckdb_database"."main"."cert_dc_insern"
)
SELECT * FROM source