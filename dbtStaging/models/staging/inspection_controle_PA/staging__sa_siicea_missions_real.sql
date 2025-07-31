{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source(get_source_schema(), 'sa_siicea_missions_real') }}