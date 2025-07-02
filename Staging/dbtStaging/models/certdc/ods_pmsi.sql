{{ config(
    materialized='view'
) }}


WITH mco_ssr_had_psy AS (
    SELECT 
        source,
        finess,
        annee,
        mois,
        SUM(deces_nb) AS deces_nb
    FROM (
        SELECT
            'mco_ssr_had_psy' AS source,
            SUBSTR(finess, 1, 9) AS finess,
	        CAST(SUBSTR(mois, (LENGTH(mois)-3), 4) AS VARCHAR) AS annee,
            CASE 
                WHEN mois LIKE 'Jan%' THEN 1
                WHEN mois LIKE 'Fév%' THEN 2
                WHEN mois LIKE 'Mar%' THEN 3
                WHEN mois LIKE 'Avr%' THEN 4
                WHEN mois LIKE 'Mai%' THEN 5
                WHEN mois LIKE 'juin%' THEN 6
                WHEN mois LIKE 'juil%' THEN 7
                WHEN mois LIKE 'Août%' THEN 8
                WHEN mois LIKE 'Sept%' THEN 9
                WHEN mois LIKE 'Oct%' THEN 10
                WHEN mois LIKE 'Nov%' THEN 11
                WHEN mois LIKE 'Déc%' THEN 12
            END AS mois,
            (
                somme_de_mco_hors_seances
                +somme_de_mco_seances
                +somme_de_ssr
                +somme_de_had
                +somme_de_psy)
            AS deces_nb
        FROM {{ ref('staging__sa_pmsi') }}
        WHERE 1 = 1
        AND finess != 'Non précisée'
        ) sa_pmsi_tmp
    GROUP BY 
        source,
        finess,
        annee,
        mois
)
--
--
--
-- rpu 2022 2023
, rpu AS (
    SELECT 
        source,
        finess,
        annee,
        mois,
        SUM(deces_nb) AS deces_nb
    FROM (
        SELECT 
            'rpu' AS source,
            SUBSTR(finess, 1, 9) AS finess,
	        CAST(SUBSTR(mois, (LENGTH(mois)-3), 4) AS INTEGER) AS annee,
            CASE 
                WHEN mois LIKE 'Jan%' THEN 1
                WHEN mois LIKE 'Fév%' THEN 2
                WHEN mois LIKE 'Mar%' THEN 3
                WHEN mois LIKE 'Avr%' THEN 4
                WHEN mois LIKE 'Mai%' THEN 5
                WHEN mois LIKE 'juin%' THEN 6
                WHEN mois LIKE 'juil%' THEN 7
                WHEN mois LIKE 'Août%' THEN 8
                WHEN mois LIKE 'Sept%' THEN 9
                WHEN mois LIKE 'Oct%' THEN 10
                WHEN mois LIKE 'Nov%' THEN 11
                WHEN mois LIKE 'Déc%' THEN 12
            END AS mois,
            total
            AS deces_nb
        FROM {{ ref('staging__sa_rpu') }}
        WHERE 1 = 1
        AND finess != 'Non précisée') sa_rpu_tmp
    GROUP BY
        source,
        finess,
        annee,
        mois
)
--
--
--
--esms 2022
, esms AS (
    SELECT 
        source,
        finess,
        annee,
        mois,
        SUM(deces_nb) AS deces_nb
    FROM (
        SELECT 
            'esms' AS source,
            SUBSTR(finess, 1, 9) AS finess,
            annee AS annee,
            '' AS mois,
            total AS deces_nb
        FROM {{ ref('staging__sa_esms') }}
        ) sa_esms_tmp
    GROUP BY
        source,
        finess,
        annee,
        mois
)
--
--
--
--usld
, usld AS (
    SELECT 
        source,
        finess,
        annee,
        mois,
        SUM(deces_nb) AS deces_nb
    FROM (
        SELECT 
            'usld' AS source,
            SUBSTR(finess, 1, 9) AS finess,
            annee AS annee,
            '' AS mois,
            total AS deces_nb
        FROM {{ ref('staging__sa_usld') }}
        WHERE 1 = 1
        AND finess != 'Non précisée'
        ) sa_ulsd_tmp
    GROUP BY 
        source,
        finess,
        annee,
        mois
    )
--
--
--
SELECT *
FROM (
    SELECT * FROM mco_ssr_had_psy
    UNION
    SELECT * FROM rpu
    UNION
    SELECT * FROM esms
    UNION
    SELECT * FROM usld
) union_all
WHERE annee != 'USLD'