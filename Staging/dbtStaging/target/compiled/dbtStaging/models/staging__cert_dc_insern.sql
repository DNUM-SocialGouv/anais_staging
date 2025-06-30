

WITH insern AS (
    SELECT * FROM "staging"."public"."staging__sa_insern"
)

SELECT 
    ROW_NUMBER() OVER () AS index,
    CAST(Annee || '-' || LPAD(Mois::VARCHAR, 2, '0') || '-' || LPAD(Jour::VARCHAR, 2, '0') AS DATE) AS date_deces,
    Annee AS annee,
    Mois AS mois,
    Jour AS jour,
    departement_code,
    commune_code,
    et_finess,
    lieu_de_deces,
    source,
    1 AS deces_nb
FROM insern