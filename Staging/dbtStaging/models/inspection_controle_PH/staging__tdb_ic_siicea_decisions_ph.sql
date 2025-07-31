-- TdBICSiiceaDecisionsPH source
{{ config(
    materialized='view'
) }}

SELECT
    mission_id,
    type_de_decision AS decision_type,
    complement AS decision_complement,
    theme_decision AS decision_theme,
    sous_theme_decision AS decision_sous_theme,
    ecart_constate AS ecart_constate,
    conformite AS conformite,
    nombre AS nombre,
    statut_de_decision AS decision_statut
FROM
{{ ref('staging__tdb_ic_siicea_mission_ph') }} missions
LEFT JOIN
{{ ref('staging__sa_siicea_decisions') }} decisions ON missions.mission_id = decisions.identifiant_de_la_mission