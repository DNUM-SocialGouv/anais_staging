{{ config(
    materialized='view'
) }}
{% set reference_date = '2025-04-01' %}

WITH missions AS (
    SELECT 
        *,
        CASE 
            WHEN LENGTH(finess_geographique) = 8 THEN '0' || finess_geographique
            ELSE finess_geographique
        END AS cd_finess,
        {{ dbtStaging.get_first_day_of_x_years_ago(3, reference_date) }} as testo,
        {{ dbtStaging.get_yesterday(reference_date) }} as testa
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
        CAST(SUBSTRING(date_reelle_visite, 7, 4) || '-' ||
             SUBSTRING(date_reelle_visite, 4, 2) || '-' ||
             SUBSTRING(date_reelle_visite, 1, 2) AS VARCHAR)
        BETWEEN '{{ dbtStaging.get_first_day_of_x_years_ago(3, reference_date) }}' AND '{{ dbtStaging.get_yesterday(reference_date) }}' -- A confirmer '2022-01-01' AND '2025-07-31'
    )
)

SELECT * FROM missions