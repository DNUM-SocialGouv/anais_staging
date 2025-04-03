-- models/staging/finess/stg_cert_dc_tfiness.sql
WITH source AS (
    SELECT 
        identifiant_certificat,
        code_finess,
        date_certificat,
        statut_certificat,
        COUNT(*) OVER () AS total_certificats,
        RANK() OVER (PARTITION BY code_finess ORDER BY date_certificat DESC) AS rang_certificat
    FROM "duckdb_database"."main"."cert_dc_tfiness"
)
SELECT * FROM source