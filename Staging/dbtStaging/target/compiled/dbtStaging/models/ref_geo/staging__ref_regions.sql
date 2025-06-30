

WITH regions AS (
    SELECT
        REG AS reg,
        NCC AS ncc,
        LIBELLE AS libelle
    FROM "staging"."public"."staging__v_region"

    UNION

    SELECT
        COMER AS reg,
        NCC AS ncc,
        LIBELLE AS libelle
    FROM "staging"."public"."staging__v_comer" 
)

SELECT * FROM regions