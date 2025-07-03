{{ config(
    materialized='view'
) }}

WITH finess_categories AS (
    SELECT 
        finess,
        {{ iif_replacement(
            "categ_niv2_code = '1100'
            OR categ_niv2_code = '1200'",
            "categ_niv2_code",
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
            OR categ_niv2_code = '1200'",
            "categ_niv2_lib",
                iif_replacement("categ_niv3_code = '2103'
                OR categ_niv3_code = '2205'
                OR categ_niv3_code = '3406'
                OR categ_niv3_code = '3407'
                OR categ_niv3_code = '4401'
                OR categ_niv3_code = '4402'
                OR categ_niv3_code = '4404'",
                "categ_niv3_lib",
                    iif_replacement("categ_code = '433'
                    OR categ_code = '188'",
                    "categ_lib",
                    "'Autre'")
                )
            ) }} AS CAT_ETAB_LB
    FROM {{ ref('staging__sa_t_finess') }} t_finess
)
, brut_insern_edc AS (
    SELECT
        ref_insee_region.reg AS REG_CD,
        ref_insee_region.libelle AS REG_LB,
        ref_insee_departement.dep AS DEP_CD,
        ref_insee_departement.libelle AS DEP_LB,
        t_finess.com_code AS COM_CD,
        communes.libelle AS COM_LB,
        t_finess.ej_finess AS EJ_FINESS_CD,
        t_finess.ej_rs AS EJ_FINESS_LB,
        t_finess.categ_code AS CAT_ETAB_CD,
        t_finess.categ_lib AS CAT_ETAB_LB,
        {{ iif_replacement("LENGTH(ods_insern.et_finess)=9","CAST(ods_insern.et_finess AS VARCHAR)","'0'||CAST(ods_insern.et_finess AS VARCHAR)") }} AS FINESS_CD,
        t_finess.rs AS FINESS_LB,
        annee AS DECES_ANNEE,
        mois AS DECES_MOIS,
        jour AS DECES_JOUR,
        lieu_de_deces AS DECES_LIEU,
        ods_insern.source AS DECES_SRC,
        deces_nb AS DECES_NB
    FROM {{ ref('ods_insern') }} ods_insern
    LEFT JOIN {{ ref('staging__sa_t_finess') }} t_finess ON {{ iif_replacement("LENGTH(ods_insern.et_finess)=9","CAST(ods_insern.et_finess AS VARCHAR)","'0'||CAST(ods_insern.et_finess AS VARCHAR)") }} = t_finess.finess
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
        ) communes ON t_finess.com_code = communes.com
    LEFT JOIN {{ ref('staging__v_departement') }} ref_insee_departement ON communes.dep = ref_insee_departement.dep 
    LEFT JOIN {{ ref('staging__v_region') }} ref_insee_region ON ref_insee_departement.reg = ref_insee_region.reg 
    WHERE 1=1 
    AND LENGTH({{ iif_replacement("LENGTH(ods_insern.et_finess)=9","CAST(ods_insern.et_finess AS VARCHAR)","'0'||CAST(ods_insern.et_finess AS VARCHAR)") }}) = 9
)
, agg_annee_insern_edc AS (
	SELECT 
        FINESS_CD || DECES_ANNEE AS ID_REF,
        REG_CD,
        REG_LB,
        DEP_CD,
        DEP_LB,
        COM_CD,
        COM_LB,
        EJ_FINESS_CD,
        EJ_FINESS_LB,
        CAT_ETAB_CD,
        CAT_ETAB_LB,
        FINESS_CD,
        FINESS_LB,
        DECES_ANNEE,
        SUM(DECES_NB) AS EDC_NB
	FROM brut_insern_edc
	GROUP BY
        FINESS_CD || DECES_ANNEE,
        REG_CD,
        REG_LB,
        DEP_CD,
        DEP_LB,
        COM_CD,
        COM_LB,
        EJ_FINESS_CD,
        EJ_FINESS_LB,
        CAT_ETAB_CD,
        CAT_ETAB_LB,
        FINESS_CD,
        FINESS_LB,
        DECES_ANNEE
)
, brut_pmsi_deces AS (
	SELECT 
        ref_insee_region.reg AS REG_CD,
        ref_insee_region.libelle AS REG_LB,
        ref_insee_departement.dep AS DEP_CD,
        ref_insee_departement.libelle AS DEP_LB,
        t_finess.com_code AS COM_CD,
        communes.libelle AS COM_LB,
        t_finess.ej_finess AS EJ_FINESS_CD,
        t_finess.ej_rs AS EJ_FINESS_LB,
        t_finess.categ_code AS CAT_ETAB_CD,
        t_finess.categ_lib AS CAT_ETAB_LB,
        ods_pmsi.finess AS FINESS_CD,
        t_finess.rs AS FINESS_LB,
        ods_pmsi.annee AS DECES_ANNEE,
        ods_pmsi.mois AS DECES_MOIS,
        ods_pmsi.source AS DECES_SOURCE,
        ods_pmsi.deces_nb AS DECES_NB
	FROM {{ ref('ods_pmsi') }} ods_pmsi
	LEFT JOIN {{ ref('staging__sa_t_finess') }} t_finess ON {{ iif_replacement("LENGTH(ods_pmsi.finess)=8", "'0' || CAST(ods_pmsi.finess AS VARCHAR)", "CAST(ods_pmsi.finess AS VARCHAR)") }} = t_finess.finess
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
	) communes ON t_finess.com_code = communes.com
	LEFT JOIN {{ ref('staging__v_departement') }} ref_insee_departement ON communes.dep = ref_insee_departement.dep 
	LEFT JOIN {{ ref('staging__v_region') }} ref_insee_region ON ref_insee_departement.reg = ref_insee_region.reg 
)
, agg_annee_pmsi_deces AS (
	SELECT 
        FINESS_CD || DECES_ANNEE AS ID_REF,
        REG_CD,
        REG_LB,
        DEP_CD,
        DEP_LB,
        COM_CD,
        COM_LB,
        EJ_FINESS_CD,
        EJ_FINESS_LB,
        CAT_ETAB_CD,
        CAT_ETAB_LB,
        FINESS_CD,
        FINESS_LB,
        DECES_ANNEE,
        SUM(DECES_NB) AS DECES_NB
	FROM brut_pmsi_deces
	GROUP BY
        FINESS_CD || DECES_ANNEE,
        REG_CD,
        REG_LB,
        DEP_CD,
        DEP_LB,
        COM_CD,
        COM_LB,
        EJ_FINESS_CD,
        EJ_FINESS_LB,
        CAT_ETAB_CD,
        CAT_ETAB_LB,
        FINESS_CD,
        FINESS_LB,
        DECES_ANNEE
)
, base AS (
    SELECT
        ID_REF
    FROM agg_annee_pmsi_deces
    UNION
    SELECT
        ID_REF
    FROM agg_annee_insern_edc
)
, agg_all AS (
	SELECT 
        base.ID_REF,
        {{ iif_replacement("agg_annee_pmsi_deces.REG_CD IS NOT NULL", "agg_annee_pmsi_deces.REG_CD", "agg_annee_insern_edc.REG_CD") }} AS REG_CD,
        {{ iif_replacement("agg_annee_pmsi_deces.REG_LB IS NOT NULL", "agg_annee_pmsi_deces.REG_LB", "agg_annee_insern_edc.REG_LB") }} AS REG_LB,
        {{ iif_replacement("agg_annee_pmsi_deces.DEP_CD IS NOT NULL", "agg_annee_pmsi_deces.DEP_CD", "agg_annee_insern_edc.DEP_CD") }} AS DEP_CD,
        {{ iif_replacement("agg_annee_pmsi_deces.DEP_LB IS NOT NULL", "agg_annee_pmsi_deces.DEP_LB", "agg_annee_insern_edc.DEP_LB") }} AS DEP_LB,
        {{ iif_replacement("agg_annee_pmsi_deces.COM_CD IS NOT NULL", "agg_annee_pmsi_deces.COM_CD", "agg_annee_insern_edc.COM_CD") }} AS COM_CD,
        {{ iif_replacement("agg_annee_pmsi_deces.COM_LB IS NOT NULL", "agg_annee_pmsi_deces.COM_LB", "agg_annee_insern_edc.COM_LB") }} AS COM_LB,
        {{ iif_replacement("agg_annee_pmsi_deces.EJ_FINESS_CD IS NOT NULL", "agg_annee_pmsi_deces.EJ_FINESS_CD", "agg_annee_insern_edc.EJ_FINESS_CD") }} AS EJ_FINESS_CD,
        {{ iif_replacement("agg_annee_pmsi_deces.EJ_FINESS_LB IS NOT NULL", "agg_annee_pmsi_deces.EJ_FINESS_LB", "agg_annee_insern_edc.EJ_FINESS_LB") }} AS EJ_FINESS_LB,
        {{ iif_replacement("agg_annee_pmsi_deces.CAT_ETAB_CD IS NOT NULL", "agg_annee_pmsi_deces.CAT_ETAB_CD", "agg_annee_insern_edc.CAT_ETAB_CD") }} AS CAT_ETAB_CD,
        {{ iif_replacement("agg_annee_pmsi_deces.CAT_ETAB_LB IS NOT NULL", "agg_annee_pmsi_deces.CAT_ETAB_LB", "agg_annee_insern_edc.CAT_ETAB_LB") }} AS CAT_ETAB_LB,
        {{ iif_replacement("agg_annee_pmsi_deces.FINESS_CD IS NOT NULL", "agg_annee_pmsi_deces.FINESS_CD", "agg_annee_insern_edc.FINESS_CD") }} AS FINESS_CD,
        {{ iif_replacement("agg_annee_pmsi_deces.FINESS_LB IS NOT NULL", "agg_annee_pmsi_deces.FINESS_LB", "agg_annee_insern_edc.FINESS_LB") }} AS FINESS_LB,
        {{ iif_replacement("agg_annee_pmsi_deces.DECES_ANNEE IS NOT NULL", "CAST(agg_annee_pmsi_deces.DECES_ANNEE AS VARCHAR)", "CAST(agg_annee_insern_edc.DECES_ANNEE AS VARCHAR)") }} AS DECES_ANNEE,
        EDC_NB,
        DECES_NB
	FROM base 
	LEFT JOIN agg_annee_pmsi_deces ON base.ID_REF = agg_annee_pmsi_deces.ID_REF
	LEFT JOIN agg_annee_insern_edc ON base.id_ref = agg_annee_insern_edc.id_ref
)
, tmp_categories AS (
    SELECT 
        FINESS_CD,
        FINESS_LB,
        DECES_ANNEE,
        SUM(EDC_NB) AS EDC_NB,
        SUM(DECES_NB) AS DECES_NB
    FROM agg_all
    WHERE CAST(DECES_ANNEE AS INTEGER) = 2023
    GROUP BY
        FINESS_CD,
        FINESS_LB,
        DECES_ANNEE
)
--
--
--
, categories AS (
SELECT 
    FINESS_CD,
    FINESS_LB,
    DECES_ANNEE,
    {{ iif_replacement("EDC_NB IS NULL", "0", "EDC_NB") }} AS EDC_NB,
    DECES_NB,
    {{ iif_replacement("DECES_NB < 100",
                "'<100'",
                iif_replacement("DECES_NB >= 100 AND DECES_NB < 250",
                    "'>=100 et <250'",
                    iif_replacement("DECES_NB >= 250 AND DECES_NB < 500",
                        "'>=250 et <500'",
                        iif_replacement("DECES_NB >= 500 AND DECES_NB < 1000",
                            "'>=500 et <1000'",
                            iif_replacement("DECES_NB >= 1000",
                                "'>=1000'",
                                "''"
                        )
                    )
                )
            )
        ) }} AS CATEGORIE_NB_DECES,
    {{ iif_replacement(
        "EDC_NB IS NULL AND DECES_NB IS NOT NULL",
            "'0%'",
            iif_replacement("ROUND(((EDC_NB::numeric / DECES_NB::numeric) * 100), 2) < 50",
                "'<50%'",
                iif_replacement("ROUND(((EDC_NB::numeric / DECES_NB::numeric) * 100), 2) >= 50 AND ROUND(((EDC_NB::numeric / DECES_NB::numeric) * 100), 2) < 70",
                    "'>=50% et <70%'",
                    iif_replacement("ROUND(((EDC_NB::numeric / DECES_NB::numeric) * 100), 2) >= 70 AND ROUND(((EDC_NB::numeric / DECES_NB::numeric) * 100), 2) < 90",
                        "'>=70% et <90%'",
                        iif_replacement("ROUND(((EDC_NB::numeric / DECES_NB::numeric) * 100), 2) >= 90 AND ROUND(((EDC_NB::numeric / DECES_NB::numeric) * 100), 2) < 100",
                            "'>=90%'",
                            "'N.A.'"		
                    )
                )
            )
        )
    ) }} AS CATEGORIE_TAUX_EDC
    FROM tmp_categories
)

SELECT
    agg_all.ID_REF,
    agg_all.REG_CD,
    agg_all.REG_LB,
    agg_all.DEP_CD,
    agg_all.DEP_LB,
    agg_all.COM_CD,
    agg_all.COM_LB,
    agg_all.EJ_FINESS_CD,
    agg_all.EJ_FINESS_LB,
    finess_categories.CAT_ETAB_CD,
    finess_categories.CAT_ETAB_LB,
    agg_all.FINESS_CD,
    agg_all.FINESS_LB,
    agg_all.DECES_ANNEE,
    categories.CATEGORIE_NB_DECES,
    categories.CATEGORIE_TAUX_EDC,
    agg_all.EDC_NB,
    agg_all.DECES_NB
FROM agg_all
LEFT JOIN categories ON agg_all.FINESS_CD = categories.FINESS_CD
LEFT JOIN finess_categories ON agg_all.FINESS_CD = finess_categories.finess