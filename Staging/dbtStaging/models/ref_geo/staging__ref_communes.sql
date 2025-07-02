{{ config(
    materialized='view'
) }}
 
WITH all_com AS (
    -- Communes actuelles
    SELECT
        COM AS com,
        REG AS reg,
        DEP AS dep,
        NCC AS ncc,
        LIBELLE AS libelle
    FROM {{ ref('staging__v_commune') }}
    WHERE REG != ''
 
    UNION
 
    -- Communes des DROM
    SELECT
        COM_COMER AS com,
        COMER AS reg,
        COMER AS dep,
        NCC AS ncc,
        LIBELLE AS libelle
    FROM {{ ref('staging__v_commune_comer') }}
 
    UNION
 
    -- Communes historiques
    SELECT
        vcd.COM AS com,
        rd.reg AS reg,
        CASE
            WHEN CAST(vcd.COM AS INTEGER) >= 97000 THEN SUBSTRING(vcd.COM, 1, 3)
            ELSE SUBSTRING(vcd.COM, 1, 2)
        END AS dep,
        vcd.NCC AS ncc,
        vcd.LIBELLE AS libelle
    FROM {{ ref('staging__v_commune_depuis') }} vcd
    LEFT JOIN {{ ref('staging__ref_departements') }} rd
        ON (CASE
                WHEN CAST(vcd.COM AS INTEGER) >= 97000 THEN SUBSTRING(vcd.COM, 1, 3)
                ELSE SUBSTRING(vcd.COM, 1, 2)
            END) = rd.dep
)
 
-- Suppression des doublons
SELECT DISTINCT * FROM all_com