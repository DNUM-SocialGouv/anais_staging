{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source(dbtStaging.get_source_schema(), 'sa_t_finess') }}