

WITH siicea_cibles AS (
    SELECT 
        "FINESS" AS code_finess,
        "RPPSS" AS code_rppss,
        "SIRET" AS code_siret,
        "Code APE" AS code_ape,
        "Code UAI" AS code_uai,
        "Groupe de cibles" AS groupe_cibles,
        "Nom" AS nom_cible,
        "Departement" AS departement,
        "Commune" AS commune,
        "Adresse" AS adresse,
        "Nombre de missions réalisées " AS nb_missions_realisees,
        "Nombre de missions abondonnées / reportées " AS nb_missions_abandonnees,
        "Nombre de décisions prises" AS nb_decisions_prises
    FROM "staging"."public"."staging__sa_siicea_cibles"
    
    WHERE TRIM("FINESS") != ''  -- Exclure les lignes sans code FINESS
)

SELECT * FROM siicea_cibles