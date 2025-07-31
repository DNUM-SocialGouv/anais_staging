{{ config(
    materialized='view'
) }}

WITH siicea_cibles AS (
    SELECT 
        finess AS code_finess,
        rpps AS code_rppss,
        siret AS code_siret,
        code_ape,
        code_uai,
        groupe_cibles,
        nom_cible,
        code_departement AS departement,
        commune,
        adresse,
        nb_mission_realisees,
        nb_mission_abandonnees,
        nb_decisions AS nb_decisions_prises
    FROM {{ ref('staging__sa_siicea_cibles') }}
    
    WHERE TRIM(finess) != ''  -- Exclure les lignes sans code FINESS
)

SELECT * FROM siicea_cibles