{{ config(
    materialized='view'
) }}


WITH brut_insee_deces AS (
    SELECT 
        REGION_CD
        || DEP_CD 
        || COM_CD 
        || DECES_ANNEE 
        || {{ iif_replacement("LENGTH(CAST(DECES_MOIS AS VARCHAR)) = 1", "'0' || CAST(DECES_MOIS AS VARCHAR)", "CAST(DECES_MOIS AS VARCHAR)") }}
        || {{ iif_replacement("LENGTH(CAST(DECES_JOUR AS VARCHAR)) = 1", "'0' || CAST(DECES_JOUR AS VARCHAR)", "CAST(DECES_JOUR AS VARCHAR)") }}
        AS ID_REF,
        REGION_CD AS REG_CD,
        REGION_LB AS REG_LB,
        DEP_CD,
        DEP_LB,
        COM_CD,
        COM_LB,
        DECES_ANNEE,
        DECES_MOIS,
        DECES_JOUR,
        SUM(DECES_NB) AS DECES_NB
    FROM {{ ref('ods_insee') }}
    GROUP BY
        REGION_CD,
        REGION_LB,
        DEP_CD,
        DEP_LB,
        COM_CD,
        COM_LB,
        DECES_ANNEE,
        DECES_MOIS,
        DECES_JOUR
)
--
--
--
, brut_insern_edc AS (
	SELECT 
        {{ iif_replacement(
            "(ref_insee_region.REG 
            || ref_insee_departement.dep  
            || communes.com 
            || Annee 
            || " ~ iif_replacement("LENGTH(CAST(Mois AS VARCHAR)) = 1", "'0' || CAST(Mois AS VARCHAR)", "CAST(Mois AS VARCHAR)") ~ "
            || " ~ iif_replacement("LENGTH(CAST(Jour AS VARCHAR)) = 1", "'0' || CAST(Jour AS VARCHAR)", "CAST(Jour AS VARCHAR)") ~ "IS NULL)",
        "'NULL'",
        "(
            ref_insee_region.REG 
            || ref_insee_departement.dep  
            || communes.com 
            || Annee 
            || " ~ iif_replacement("LENGTH(CAST(Mois AS VARCHAR)) = 1", "'0' || CAST(Mois AS VARCHAR)", "CAST(Mois AS VARCHAR)") ~ "
            || " ~ iif_replacement("LENGTH(CAST(Jour AS VARCHAR)) = 1", "'0' || CAST(Jour AS VARCHAR)", "CAST(Jour AS VARCHAR)") ~ 
            ")"
            ) }}
        AS ID_REF,
        ref_insee_region.reg AS REG_CD,
        ref_insee_region.libelle AS REG_LB,
        ref_insee_departement.dep AS DEP_CD,
        ref_insee_departement.libelle AS DEP_LB,
        communes.com AS COM_CD,
        communes.libelle AS COM_LB,
        annee AS DECES_ANNEE,
        mois AS DECES_MOIS,
        jour AS DECES_JOUR,
        SUM(deces_nb) AS DECES_NB
	FROM {{ ref('ods_insern') }} ods_insern
	LEFT JOIN (
		SELECT 
            CAST(com AS VARCHAR) AS com,
            libelle,
            MAX(SUBSTR(com, 1, 2)) AS dep
		FROM {{ ref('staging__v_commune') }} ref_insee_communes
		WHERE 1=1
		GROUP BY
            com,
            libelle
		UNION 
		SELECT
            CAST(com AS VARCHAR) AS com,
            libelle,
            MAX(SUBSTR(com, 1, 2)) AS dep
		FROM {{ ref('staging__v_commune_depuis') }} ref_insee_communes
		WHERE 1=1
		AND com NOT IN (
			SELECT 
			    com
			FROM {{ ref('staging__v_commune') }} ref_insee_communes
			WHERE 1=1
			)
		GROUP BY
            com,
            libelle
		) communes ON (departement_code || commune_code) = communes.com
	LEFT JOIN {{ ref('staging__v_departement') }} ref_insee_departement ON departement_code = ref_insee_departement.dep 
	LEFT JOIN {{ ref('staging__v_region') }} ref_insee_region ON ref_insee_departement.REG = ref_insee_region.reg 
	LEFT JOIN {{ ref('staging__sa_t_finess') }} t_finess ON ods_insern.et_finess = t_finess.finess
	GROUP BY 
        ref_insee_region.reg,
        ref_insee_region.libelle,
        ref_insee_departement.dep,
        ref_insee_departement.libelle,
        communes.com,
        communes.libelle,
        annee,
        mois,
        jour
	)
--
--
--
, base AS (
    SELECT 
        ID_REF 
    FROM brut_insee_deces
    UNION 
    SELECT 
        ID_REF 
    FROM brut_insern_edc
)
--
--
--	
SELECT 
--SUM(brut_insern_edc.DECES_NB)
--, SUM(brut_insee_deces.DECES_NB)
    base.ID_REF,
    {{ iif_replacement("brut_insee_deces.REG_CD IS NOT NULL", "brut_insee_deces.REG_CD", "brut_insern_edc.REG_CD") }} AS REG_CD,
    {{ iif_replacement("brut_insee_deces.REG_LB IS NOT NULL", "brut_insee_deces.REG_LB", "brut_insern_edc.REG_LB") }} AS REG_LB,
    {{ iif_replacement("brut_insee_deces.DEP_CD IS NOT NULL", "brut_insee_deces.DEP_CD", "brut_insern_edc.DEP_CD") }} AS DEP_CD,
    {{ iif_replacement("brut_insee_deces.DEP_LB IS NOT NULL", "brut_insee_deces.DEP_LB", "brut_insern_edc.DEP_LB") }} AS DEP_LB,
    {{ iif_replacement("brut_insee_deces.COM_CD IS NOT NULL", "brut_insee_deces.COM_CD", "brut_insern_edc.COM_CD") }} AS COM_CD,
    {{ iif_replacement("brut_insee_deces.COM_LB IS NOT NULL", "brut_insee_deces.COM_LB", "brut_insern_edc.COM_LB") }} AS COM_LB,
    {{ iif_replacement("brut_insee_deces.DECES_ANNEE IS NOT NULL", "brut_insee_deces.DECES_ANNEE", "brut_insern_edc.DECES_ANNEE") }} AS DECES_ANNEE,
    {{ iif_replacement("brut_insee_deces.DECES_MOIS IS NOT NULL", "brut_insee_deces.DECES_MOIS", "brut_insern_edc.DECES_MOIS") }} AS DECES_MOIS,
    {{ iif_replacement("brut_insee_deces.DECES_JOUR IS NOT NULL", "brut_insee_deces.DECES_JOUR", "brut_insern_edc.DECES_JOUR") }} AS DECES_JOUR,
    brut_insern_edc.DECES_NB AS EDC_NB,
    brut_insee_deces.DECES_NB AS DECES_TOT_NB
FROM base
LEFT JOIN brut_insee_deces ON base.ID_REF = brut_insee_deces.ID_REF
LEFT JOIN brut_insern_edc ON base.ID_REF = brut_insern_edc.ID_REF
