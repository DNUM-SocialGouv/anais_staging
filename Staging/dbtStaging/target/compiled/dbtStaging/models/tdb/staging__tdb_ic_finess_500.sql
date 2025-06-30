

WITH finess_500 AS (
    SELECT 
        finess,
        rs,
        CASE 
            WHEN LENGTH(com_code) = 4 THEN '0' || com_code 
            ELSE com_code 
        END AS com_code,
        statut_jur_niv2_code,
        statut_jur_niv2_lib,
        etat
    FROM "staging"."public"."staging__sa_t_finess"
    WHERE categ_code = 500
    AND etat = 'ACTUEL'
)

SELECT * FROM finess_500