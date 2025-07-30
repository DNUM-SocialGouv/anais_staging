-- TdBICSiiceaMissionsPH source
{{ config(
    materialized='view'
) }}


WITH etab AS (
    SELECT
        finess,
        rs,
        ej_finess,
        ej_rs,
        categ_code,
        categ_lib,
        statut_jur_code,
        statut_jur_lib
    FROM
    -- t_finess du 7 juillet 2025
    {{ ref('staging__sa_t_finess') }}
    WHERE
    -- catégories PH
    -- source : catégories sélectionnées par Nicolas
    -- on filtre ici et pas dans la requête finale afin de contrôler les cas où on ne retrouve pas le finess dans le référentiel ou finess non renseigné dans SIICEA
    categ_code IN (
    "182",
    "183",
    "186",
    "188",
    "189",
    "190",
    "192",
    "194",
    "195",
    "196",
    "198",
    "221",
    "238",
    "246",
    "249",
    "255",
    "370",
    "377",
    "379",
    "390",
    "395",
    "396",
    "402",
    "437",
    "445",
    "448"
    )
)
, tdb_esms AS (
    SELECT
    -- normalisation du finess
        CASE
            WHEN LENGTH(finess_geographique)=8 THEN '0' || finess_geographique
            ELSE finess_geographique
        END AS finess_geographique,
        -- déficiences : 1 = secondaire, sinon primaire
        deficiences_auditives,
        deficiences_auditives1,
        deficiences_intellectuelles,
        deficiences_intellectuelles1,
        deficiences_metaboliques_viscerales_et_nutritionnelles,
        deficiences_metaboliques_viscerales_et_nutritionnelles1,
        deficiences_motrices,
        deficiences_motrices1,
        deficiences_visuelles,
        deficiences_visuelles1,
        autres_types_de_deficiences,
        autres_types_de_deficiences1
    FROM
    {{ ref('staging__matrice_tdb_esms') }}
)
, matrice AS (
    SELECT
        finess_geographique,
        Internat, -- hébergement
        TMA, -- hébergement ou non
        Externat, -- pas d'hébergement
        MO, -- pas d'hébergement
        Autre,
        public-- source : les ARS ont flagué adulte / enfant lors des travaux sur la matrice de préciblage
    FROM
    {{ ref('staging__matrice_ciblage') }}
)
, missions AS (
    SELECT
        DISTINCT identifiant_de_la_mission,
        -- normalisation du finess
        CASE
        WHEN LENGTH(finess_geographique)=8 THEN '0' || finess_geographique
        ELSE finess_geographique
        END AS finess_geographique,
        code_theme_igas,
        theme_igas,
        modalite_d_investigation,
        type_de_planification,
        modalite_de_la_mission,
        mission_conjointe_1,
        mission_conjointe_2,
        groupe_de_cibles,
        Cible,
        categorie_de_cible,
        date_reelle_visite,
        statut_de_la_mission
    FROM
    {{ ref('staging__sa_siicea_missions_real') }}
    WHERE
        1=1
        -- filtre sur les types de mission (on ne conserve que les types inspeciton et contrôle)
        AND
        type_de_mission NOT IN (
        'Audit',
        'Audit franco-wallon',
        'Evaluation',
        'Visites de conformité',
        'Enquête administrative'
        )
        -- filtre des thèmes IGAS (on ne conserve que ce qui concerne les soins)
        AND
        theme_igas IN (
        "ESMS - Prise en charge des Adultes handicapés",
        "ESMS - Prise en charge des Enfants handicapés",
        "ESMS - enfants handicapés - droits, individualisation des parcours",
        "ONIC ESMS - PH adultes",
        "ONIC ESMS - PH enfants"
        )
        -- filtre sur la période
        AND (
        CAST((SUBSTR(date_reelle_visite,1,4) || SUBSTR(date_reelle_visite,6,2) || SUBSTR(date_reelle_visite,9,2)) AS INTEGER)
        >=20240701
        AND
        CAST((SUBSTR(date_reelle_visite,1,4) || SUBSTR(date_reelle_visite,6,2) || SUBSTR(date_reelle_visite,9,2)) AS INTEGER)
        <=20250630
        )
        AND
        date_reelle_visite != ""
        -- filtre sur le statut réalisé
        AND
        statut_de_la_mission IN (
        "Clôturé",
        "Maintenu")
)

SELECT
    DISTINCT identifiant_de_la_mission AS mission_id,
    missions.finess_geographique AS finess_cd,
    code_theme_igas AS igas_cd,
    theme_igas AS igas_lb,
    modalite_d_investigation AS investigation,
    type_de_planification AS planification,
    modalite_de_la_mission AS modalite,
    mission_conjointe_1 AS conjointe_1,
    mission_conjointe_2 AS conjointe_2,
    groupe_de_cibles AS groupe,
    date_reelle_visite AS visite_reelle_dt,
    statut_de_la_mission AS statut,
    rs AS et_raison_sociale,
    ej_finess AS ej_finess_cd,
    ej_rs AS ej_raison_sociale,
    categ_code AS categ_cd,
    categ_lib AS categ_lb,
    statut_jur_code AS statut_juridique_cd,
    statut_jur_lib AS statut_juridique_lb,
    deficiences_auditives AS deficiences_auditives_pr_tx,
    deficiences_auditives1 AS deficiences_auditives_sd_tx,
    deficiences_intellectuelles AS deficiences_intellectuelles_pr_tx,
    deficiences_intellectuelles1 AS deficiences_intellectuelles_sd_tx,
    deficiences_metaboliques_viscerales_et_nutritionnelles AS deficiences_metaboliques_viscerales_et_nutritionnelles_pr_tx,
    deficiences_metaboliques_viscerales_et_nutritionnelles1 AS deficiences_metaboliques_viscerales_et_nutritionnelles_sd_tx,
    deficiences_motrices AS deficiences_motrices_pr_tx,
    deficiences_motrices1 AS deficiences_motrices_sd_tx,
    deficiences_visuelles AS deficiences_visuelles_pr_tx,
    deficiences_visuelles1 AS deficiences_visuelles_sd_tx,
    autres_types_de_deficiences AS autres_types_de_deficiences_pr_tx,
    autres_types_de_deficiences1 AS autres_types_de_deficiences_sd_tx,
    Internat AS internat_places_nb,
    TMA AS tma_places_nb,
    Externat AS externat_places_nb,
    MO AS mo_places_nb,
    Autre AS autres_places_nb,
    public
FROM
missions
LEFT JOIN
etab ON missions.finess_geographique = etab.finess
LEFT JOIN
tdb_esms ON missions.finess_geographique = tdb_esms.finess_geographique
LEFT JOIN
matrice ON missions.finess_geographique = matrice.finess_geographique