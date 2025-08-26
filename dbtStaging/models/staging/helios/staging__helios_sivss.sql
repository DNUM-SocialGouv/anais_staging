{{ config(
    materialized='view'
) }}

WITH sivss AS (
    SELECT 
        structure_intitule,
        numero_sivss,
        SUBSTRING(date_reception, 7, 4) || '-' || 
        SUBSTRING(date_reception, 4, 2) || '-' || 
        SUBSTRING(date_reception, 1, 2) AS date_reception,
        famille_principale,
        nature_principale,
        autre_signal_libelle,
        famille_secondaire,
        nature_secondaire,
        autre_signal_secondaire_libelle,
        est_eigs,
        consequences_personne_exposee,
        reclamation,
        declarant_est_anonyme,
        declarant_qualite_fonction,
        declarant_categorie,
        declarant_organisme_type,
        declarant_etablissement_type,
        declarant_organisme_numero_finess,
        declarant_organisme_nom,
        declarant_organisme_region,
        declarant_organisme_departement,
        declarant_organisme_code_postal,
        declarant_organisme_commune,
        declarant_organisme_code_insee,
        survenue_cas_collectivite,
        scc_organisme_type,
        scc_etablissement_type,
        scc_organisme_nom,
        scc_organisme_finess,
        scc_organisme_region,
        scc_organisme_departement,
        scc_organisme_code_postal,
        scc_organisme_commune,
        scc_organisme_code_insee,
        etat,
        support_signalement,
        SUBSTRING(date_cloture, 7, 4) || '-' || 
        SUBSTRING(date_cloture, 4, 2) || '-' || 
        SUBSTRING(date_cloture, 1, 2) AS date_cloture,
        motif_cloture
    FROM {{ ref('staging__sa_sivss') }}
    WHERE (SUBSTRING(date_cloture, 7, 4) || SUBSTRING(date_cloture, 4, 2)) <= {{ dbtStaging.get_last_month_of_last_year() }} -- A confirmer '202412'
)

SELECT * FROM sivss