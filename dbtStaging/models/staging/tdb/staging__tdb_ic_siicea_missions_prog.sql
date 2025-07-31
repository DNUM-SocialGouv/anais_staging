{{ config(
    materialized='view'
) }}

WITH missions_prog AS (
    SELECT 
        identifiant_de_la_mission AS identifiant_mission,
        code_theme_igas,
        theme_igas,
        type_de_mission AS type_mission,
        modalite_d_investigation,
        type_de_planification AS type_planification,
        modalite_de_la_mission AS modalite_mission,
        mission_conjointe_avec_1 AS mission_conjointe_1,
        mission_conjointe_avec_2 AS mission_conjointe_2,
        departement,
        commune,
        adresse,
        groupe_de_cibles AS groupe_cibles,
        cible,
        caractere_juridique,
        type_de_cible AS type_cible,
        finess_geographique AS code_finess,
        statut_de_la_mission AS statut_mission,

        -- Formatage des dates
        CASE
            WHEN LENGTH(date_provisoire_visite) = 10 
            THEN SUBSTRING(date_provisoire_visite, 7, 4) || '-' ||
                 SUBSTRING(date_provisoire_visite, 4, 2) || '-' ||
                 SUBSTRING(date_provisoire_visite, 1, 2) || ' 00:00:00'
            ELSE date_provisoire_visite
        END AS date_provisoire_visite,

        CASE
            WHEN LENGTH(date_reelle_visite) = 10 
            THEN SUBSTRING(date_reelle_visite, 7, 4) || '-' ||
                 SUBSTRING(date_reelle_visite, 4, 2) || '-' ||
                 SUBSTRING(date_reelle_visite, 1, 2) || ' 00:00:00'
            ELSE date_reelle_visite
        END AS date_reelle_visite,

        date_provisoire_fin_mission,
        date_reelle_fin_mission,

        -- Ajout du zéro devant les codes FINESS de longueur 8
        CASE 
            WHEN LENGTH(finess_geographique) = 8 THEN '0' || finess_geographique
            ELSE finess_geographique
        END AS cd_finess

    FROM {{ ref('staging__sa_siicea_missions_prog') }}
    
    WHERE 1=1
    -- Filtrage sur la période
    AND (
        (date_reelle_visite = '' 
            AND (CASE
                    WHEN LENGTH(date_provisoire_visite) = 10 
                    THEN SUBSTRING(date_provisoire_visite, 7, 4) || '-' ||
                         SUBSTRING(date_provisoire_visite, 4, 2) || '-' ||
                         SUBSTRING(date_provisoire_visite, 1, 2) || ' 00:00:00'
                    ELSE date_provisoire_visite
                 END) > '2024-12-31 00:00:00') 
        OR (
            CASE
                WHEN LENGTH(date_reelle_visite) = 10 
                THEN SUBSTRING(date_reelle_visite, 7, 4) || '-' ||
                     SUBSTRING(date_reelle_visite, 4, 2) || '-' ||
                     SUBSTRING(date_reelle_visite, 1, 2) || ' 00:00:00'
                ELSE date_reelle_visite
            END) > '2024-12-31 00:00:00'
    )
    
    -- Exclusion des statuts clôturés ou abandonnés
    AND statut_de_la_mission NOT IN ('Clôturé', 'Abandonné')
    
    --
    AND secteur_d_intervention = 'Médico-social'
    
    -- Filtrage sur le type de cible
    AND type_de_cible = 'Etablissements et Services pour Personnes Agées'
    
    -- Filtrage des thèmes IGAS pertinents
    AND code_theme_igas IN (
        'MS634D13', 'MS634D13', 'MS634N1', 'MS634E1', 
        'MS634D12', 'MS634R1', 'MS634D11', 'MS634D15', 
        'MS634C10', 'MS634C10'
    )
    
    -- Exclusion des types de mission non pertinents
    AND type_de_mission NOT IN (
        'Audit', 'Audit franco-wallon', 'Evaluation', 
        'Visites de conformité', 'Enquête administrative'
    )
)

SELECT * FROM missions_prog