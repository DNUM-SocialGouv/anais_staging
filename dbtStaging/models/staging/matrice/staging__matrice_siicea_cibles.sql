{{ config(
    materialized='view'
) }}

WITH cibles AS (
    SELECT 
        c.*,
        m.date_reelle_visite,
        m.statut_de_la_mission,
        m.secteur_d_intervention,
        m.type_de_mission
    FROM {{ ref('staging__sa_siicea_cibles') }} c
    LEFT JOIN {{ ref('staging__sa_siicea_missions_real') }} m
        ON c.FINESS = m.finess_geographique
),

filtered_cibles AS (
    SELECT *
    FROM cibles
    WHERE 
        -- Filtre sur la période (missions entre 2022 et 2024)
        date_reelle_visite BETWEEN '2022-01-01' AND '2024-12-31'
        -- Filtre sur les statuts de mission valides
        AND statut_de_la_mission IN ('Clôturé', 'Maintenu')
        -- Filtre sur le secteur d’intervention
        AND secteur_d_intervention IN ('Médico-social', 'Sanitaire')
        -- Exclusion des types de mission non pertinents
        AND type_de_mission NOT IN (
            'Audit', 'Audit franco-wallon', 
            'Evaluation', 'Visites de conformité', 
            'Enquête administrative'
        )
)

SELECT * FROM filtered_cibles