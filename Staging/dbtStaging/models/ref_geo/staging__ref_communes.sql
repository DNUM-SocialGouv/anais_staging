{{ config(
    materialized='view'
) }}

WITH all_com AS (
    -- Communes actuelles
    SELECT 
        com,
        reg,
        dep,
        ncc, 
        libelle
    FROM {{ ref('staging__v_commune') }} 
    WHERE REG != ''

    UNION 

    -- Communes des DROM
    SELECT
        com,
        reg,
        dep,
        ncc, 
        libelle
    FROM {{ ref('staging__v_commune_comer') }} 

    UNION

    -- Communes historiques
    SELECT 
        vcd.com,
        rd.reg,
        CASE 
            WHEN CAST(vcd.com AS INTEGER) >= 97000 THEN SUBSTRING(vcd.com, 1, 3)
            ELSE SUBSTRING(vcd.com, 1, 2)
        END AS dep,
        vcd.ncc, 
        vcd.libelle
    FROM {{ ref('staging__v_commune_depuis') }} vcd 
    LEFT JOIN {{ ref('staging__ref_departements') }} rd 
        ON (CASE 
                WHEN CAST(vcd.com AS INTEGER) >= 97000 THEN SUBSTRING(vcd.com, 1, 3)
                ELSE SUBSTRING(vcd.com, 1, 2)
            END) = rd.dep
)

-- Suppression des doublons
SELECT DISTINCT * FROM all_com