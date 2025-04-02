{{ config(
    materialized='view'
) }}

WITH missions_prog AS (
    SELECT 
        *
    FROM {{ ref('staging__sa_siicea_missions_prog') }}
    WHERE 
        -- Filtre sur la période (date provisoire après le 30/09/2024)
        date_provisoire_visite > '2024-09-30'
        -- Exclusion des statuts non pertinents
        AND statut_mission NOT IN ('Clôturé', 'Abandonné')
        -- Filtre sur le secteur d’intervention
        AND secteur_intervention = 'Médico-social'
        -- Filtre des thèmes IGAS pertinents
        AND code_theme_igas IN (
            'MS634D13', 'MS634N1', 'MS634E1', 
            'MS634D12', 'MS634R1', 'MS634D11', 
            'MS634D15', 'MS634C10'
        )
        -- Exclusion des types de mission non pertinents
        AND type_mission NOT IN (
            'Audit', 'Audit franco-wallon', 
            'Evaluation', 'Visites de conformité', 
            'Enquête administrative'
        )
)

SELECT * FROM missions_prog