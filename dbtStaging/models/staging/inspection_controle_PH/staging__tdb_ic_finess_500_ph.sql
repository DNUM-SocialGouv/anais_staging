-- TdBICFiness_500 source
{{ config(
    materialized='view'
) }}

SELECT
    finess,
    rs,
    com_code,
    statut_jur_niv2_code,
    statut_jur_niv2_lib,
    etat
FROM {{ ref('staging__sa_t_finess') }}
WHERE categ_code::int = 500
AND etat = 'ACTUEL'