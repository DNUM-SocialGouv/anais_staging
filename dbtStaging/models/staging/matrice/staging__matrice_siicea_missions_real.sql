{{ config(
    materialized='view'
) }}

WITH missions AS (
    SELECT 
        *
    FROM {{ ref('staging__sa_siicea_missions_real') }}
    WHERE 
        -- Filtre sur la période des missions
        date_reelle_visite BETWEEN '2022-01-01' AND '2024-12-31'
        -- Filtre sur le statut réalisé
        AND statut_de_la_mission IN ('Clôturé', 'Maintenu')
        -- Filtre sur le secteur d’intervention
        AND secteur_d_intervention = 'Médico-social'
        -- Filtre des thèmes IGAS pertinents
        AND code_theme_igas IN (
            'MS634D13', 'MS634N1', 'MS634E1', 
            'MS634D12', 'MS634R1', 'MS634D11', 
            'MS634D15', 'MS634C10'
        )
        -- Filtre sur les types de mission (on exclut les types non pertinents)
        AND type_de_mission NOT IN (
            'Audit', 'Audit franco-wallon', 
            'Evaluation', 'Visites de conformité', 
            'Enquête administrative'
        )
)

SELECT * FROM missions