{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source(dbtStaging.get_source_schema(), 'v_commune_comer') }}