{{ config(
    materialized='view'
) }}

WITH missions AS (
    SELECT 
        *
    FROM {{ ref('staging__sa_siicea_missions_real') }}
    WHERE code_theme_igas IN (
        'MS634D13', 'MS634N1', 'MS634E1', 'MS634D12', 'MS634R1',
        'MS634D11', 'MS634D15', 'MS634C10'
    )
    AND type_mission NOT IN (
        'Audit', 'Audit franco-wallon', 'Evaluation', 
        'Visites de conformité', 'Enquête administrative'
    )
    AND statut_mission IN ('Clôturé', 'Maintenu')
    AND (
        CAST(SUBSTRING("date_reelle_visite", 1, 4) || 
             SUBSTRING("date_reelle_visite", 6, 2) || 
             SUBSTRING("date_reelle_visite", 9, 2) AS INTEGER)
        BETWEEN 20220101 AND 20241231
    )
)

SELECT * FROM missions