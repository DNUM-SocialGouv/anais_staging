{{ config(
    materialized='view'
) }}

WITH missions AS (
    SELECT 
        *,
        CASE 
            WHEN LENGTH(finess_geographique) = 8 THEN '0' || finess_geographique
            ELSE finess_geographique
        END AS cd_finess
    FROM {{ ref('staging__sa_siicea_missions_real') }}
    WHERE code_theme_igas IN (
        'MS634D13', 'MS634N1', 'MS634E1', 'MS634D12', 'MS634R1',
        'MS634D11', 'MS634D15', 'MS634C10'
    )
    AND type_de_mission NOT IN (
        'Audit', 'Audit franco-wallon', 'Evaluation', 
        'Visites de conformité', 'Enquête administrative'
    )
    AND statut_de_la_mission IN ('Clôturé', 'Maintenu')
    AND (
        CAST(SUBSTRING("date_reelle_visite", 7, 4) || 
             SUBSTRING("date_reelle_visite", 4, 2) || 
             SUBSTRING("date_reelle_visite", 1, 2) AS INTEGER)
        BETWEEN 20220101 AND 20250331
    )
)

SELECT * FROM missions