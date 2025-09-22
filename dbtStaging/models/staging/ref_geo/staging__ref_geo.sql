{{ config(
    materialized='view'
) }}

WITH geo AS (
    SELECT
        rc.com AS com_cd,
        rc.libelle AS com_lb,
        rd.dep AS dep_cd,
        rd.libelle AS dep_lb,
        rr.reg AS reg_cd,
        rr.libelle AS reg_lb
    FROM {{ ref('staging__ref_communes') }} rc
    LEFT JOIN {{ ref('staging__ref_departements') }} rd
        ON rc.dep = rd.dep
    LEFT JOIN {{ ref('staging__ref_regions') }} rr
        ON rd.reg = rr.reg
)

SELECT * FROM geo