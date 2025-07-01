{{ config(
    materialized='view'
) }}

WITH missions AS (
    SELECT 
        *,
        CASE 
            WHEN LENGTH("FINESS_géographique") = 8 THEN '0' || "FINESS_géographique"
            ELSE "FINESS_géographique"
        END AS cd_finess
    FROM {{ ref('staging__sa_siicea_missions') }}
    WHERE "Code_thème_IGAS" IN (
        'MS634D13', 'MS634N1', 'MS634E1', 'MS634D12', 'MS634R1',
        'MS634D11', 'MS634D15', 'MS634C10'
    )
    AND "Type_de_mission" NOT IN (
        'Audit', 'Audit franco-wallon', 'Evaluation', 
        'Visites de conformité', 'Enquête administrative'
    )
    AND "Statut_de_la_mission" IN ('Clôturé', 'Maintenu')
    AND (
        CAST(SUBSTRING("Date_réelle__Visite_", 1, 4) || 
             SUBSTRING("Date_réelle__Visite_", 6, 2) || 
             SUBSTRING("Date_réelle__Visite_", 9, 2) AS INTEGER)
        BETWEEN 20220101 AND 20241231
    )
)

SELECT * FROM missions