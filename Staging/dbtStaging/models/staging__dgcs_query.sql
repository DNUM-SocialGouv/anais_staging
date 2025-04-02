{{ config(
    materialized='view'
) }}

WITH finess AS (
    SELECT 
        finess,
        com_code,
        categ_code,
        categ_lib,
        etat
    FROM {{ ref('staging__sa_t_finess') }}
    WHERE categ_code IN (
        448, 402, 396, 395, 370, 437, 255, 246, 198, 249, 238, 390, 188, 377, 
        192, 183, 186, 195, 194, 196, 379, 445, 221, 190, 189, 182
    )
), geo AS (
    SELECT 
        c.com AS commune_code,  -- Adapte ici si le nom est différent
        c.libelle AS commune_libelle,
        d.dep AS departement_code,
        d.libelle AS departement_libelle,
        r.reg AS region_code,
        r.libelle AS region_libelle
    FROM {{ ref('staging__v_commune') }} c
    LEFT JOIN {{ ref('staging__v_departement') }} d ON c.dep = d.dep
    LEFT JOIN {{ ref('staging__v_region') }} r ON d.reg = r.reg
)

SELECT 
    finess.*,
    geo.region_code,
    geo.region_libelle,
    geo.departement_code,
    geo.departement_libelle,
    geo.commune_libelle
FROM finess
LEFT JOIN geo ON finess.com_code = geo.commune_code