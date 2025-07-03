{{ config(
    materialized='view'
) }}
{% set reference_date = get_reference_date('2025-03-01') %}

WITH sa_insee_total AS (
    SELECT 
        *
        , 1 AS Total
    FROM {{ ref('staging__sa_insee_histo') }}
    WHERE adec::INT = {{ get_previous_year(3) }} OR adec::INT = {{ get_previous_year(2) }}
    UNION
    SELECT 
        adec,
        mdec,
        jdec,
        depdec,
        comdec,
        anais,
        mnais,
        jnais,
        sexe,
        '' AS comdom,
        '' AS lieudec2,
        --attention variables Ã  modifier selon la date de MAJ
        CASE 
            WHEN {{ previous_month_condition(reference_date, 3, 'adec', 'mdec') }} THEN 1.0115
            WHEN {{ previous_month_condition(reference_date, 2, 'adec', 'mdec') }} THEN 1.003
            WHEN {{ previous_month_condition(reference_date, 1, 'adec', 'mdec') }} THEN 1.001
            ELSE 1
        END AS Total
    FROM {{ ref('staging__DC_det') }}
    WHERE 1=1
    AND {{ previous_year_and_current(reference_date, 'adec', 'mdec') }}
    --attention variables Ã  modifier selon la date de MAJ
    --AND mdec != 7
)
--
--
--
, insee_geo AS (
    SELECT 
        ref_insee_region.reg AS REGION_CD,
        ref_insee_region.libelle AS REGION_LB,
        ref_insee_departement.dep AS DEP_CD,
        ref_insee_departement.libelle AS DEP_LB,
        communes.com AS COM_CD,
        communes.libelle AS COM_LB,
        adec AS DECES_ANNEE,
        mdec AS DECES_MOIS,
        jdec AS DECES_JOUR,
        lieudec2 AS DECES_LIEU,
        Total AS DECES_NB
    FROM sa_insee_total
    LEFT JOIN (
        SELECT 
            CAST(com AS VARCHAR) AS com,
            libelle,
            MAX(SUBSTR(com, 1, 2)) AS dep
        FROM {{ ref('staging__v_commune') }} communes
        WHERE 1=1
        GROUP BY
            com,
            libelle
        UNION 
        SELECT
            CAST(com AS VARCHAR) AS com,
            libelle, 
            MAX(SUBSTR(com, 1, 2)) AS dep
        FROM {{ ref('staging__v_commune_depuis') }}
        WHERE 1=1
        AND com NOT IN (
            SELECT 
                com
            FROM {{ ref('staging__v_commune') }} communes
            WHERE 1=1
            )
        GROUP BY
            com,
            libelle
    ) communes ON {{ iif_replacement("LENGTH(sa_insee_total.comdec) = 4", "'0' || sa_insee_total.comdec", "sa_insee_total.comdec") }} = communes.com
    LEFT JOIN {{ ref('staging__v_departement') }} ref_insee_departement ON sa_insee_total.depdec = ref_insee_departement.dep 
    --LEFT JOIN ref_insee_departement ON SUBSTR(COMDEC, 1, 2) = ref_insee_departement.DEP 
    LEFT JOIN {{ ref('staging__v_region') }} ref_insee_region ON ref_insee_departement.reg = ref_insee_region.reg 
    WHERE LENGTH(CAST(adec AS VARCHAR))=4
)
--
--
--
SELECT 
    REGION_CD,
    REGION_LB,
    DEP_CD,
    DEP_LB,
    COM_CD,
    COM_LB,
    DECES_ANNEE,
    DECES_MOIS,
    DECES_JOUR,
    DECES_LIEU,
    SUM(DECES_NB) AS DECES_NB
FROM insee_geo
GROUP BY 
    REGION_CD,
    REGION_LB,
    DEP_CD,
    DEP_LB,
    COM_CD,
    COM_LB,
    DECES_ANNEE,
    DECES_MOIS,
    DECES_JOUR,
    DECES_LIEU

