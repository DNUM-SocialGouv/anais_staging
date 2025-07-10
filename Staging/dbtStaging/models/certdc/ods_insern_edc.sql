{{ config(
    materialized='view'
) }}

WITH insern AS (
    SELECT 
        ref_insee_region.reg AS REG_CD,
        ref_insee_region.libelle AS REG_LB,
        ref_insee_departement.dep AS DEP_CD,
        ref_insee_departement.libelle AS DEP_LB,
        departement_code || commune_code AS COM_CD,
        communes.libelle AS COM_LB,
        t_finess.ej_finess AS EJ_FINESS_CD,
        t_finess.ej_rs AS EJ_FINESS_LB,
        t_finess.categ_code AS CAT_ETAB_CD,
        t_finess.categ_lib AS CAT_ETAB_LB,
        ods_insern.et_finess AS FINESS_CD,
        t_finess.rs AS FINESS_LB,
        annee AS DECES_ANNEE,
        mois AS DECES_MOIS,
        jour AS DECES_JOUR,
        lieu_de_deces AS DECES_LIEU,
        ods_insern.source AS DECES_SRC,
        deces_nb AS DECES_NB
    FROM {{ ref('ods_insern') }}
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
        ) communes ON (departement_code || commune_code) = communes.com
    LEFT JOIN {{ ref('staging__v_departement') }} ref_insee_departement ON departement_code = ref_insee_departement.dep 
    LEFT JOIN {{ ref('staging__v_region') }} ref_insee_region ON ref_insee_departement.REG = ref_insee_region.reg 
    LEFT JOIN {{ ref('staging__sa_t_finess') }} t_finess ON ods_insern.et_finess = t_finess.finess
)
, finess_categories AS (
    SELECT 
        finess,
        {{ iif_replacement("categ_niv2_code = '1100' 
            OR categ_niv2_code = '1200'", "categ_niv2_code",
            iif_replacement("categ_niv3_code = '2103'
                OR categ_niv3_code = '2205'
                OR categ_niv3_code = '3406'
                OR categ_niv3_code = '3407'
                OR categ_niv3_code = '4401'
                OR categ_niv3_code = '4402'
                OR categ_niv3_code = '4404'", "categ_niv3_code",
                iif_replacement("categ_code = '433'
                    OR categ_code = '188'", "categ_code", "'999'")
                    )
                ) }} AS CAT_ETAB_CD,
        {{ iif_replacement("categ_niv2_code = '1100'
            OR categ_niv2_code = '1200'", "categ_niv2_lib", 
                iif_replacement("categ_niv3_code = '2103'
                OR categ_niv3_code = '2205'
                OR categ_niv3_code = '3406'
                OR categ_niv3_code = '3407'
                OR categ_niv3_code = '4401'
                OR categ_niv3_code = '4402'
                OR categ_niv3_code = '4404'", "categ_niv3_lib", 
                    iif_replacement("categ_code = '433'
                    OR categ_code = '188'", "categ_lib", "'Autre'")
                )
            )}} AS CAT_ETAB_LB
    FROM {{ ref('staging__sa_t_finess') }} t_finess
)

SELECT
    insern.REG_CD,
    insern.REG_LB,
    insern.DEP_CD,
    insern.DEP_LB,
    insern.COM_CD,
    insern.COM_LB,
    insern.EJ_FINESS_CD,
    insern.EJ_FINESS_LB,
    finess_categories.CAT_ETAB_CD,
    finess_categories.CAT_ETAB_LB,
    insern.FINESS_CD,
    insern.FINESS_LB,
    insern.DECES_ANNEE,
    insern.DECES_MOIS,
    insern.DECES_JOUR,
    insern.DECES_LIEU,
    insern.DECES_SRC,
    insern.DECES_NB
FROM insern
LEFT JOIN finess_categories ON insern.FINESS_CD = finess_categories.finess
