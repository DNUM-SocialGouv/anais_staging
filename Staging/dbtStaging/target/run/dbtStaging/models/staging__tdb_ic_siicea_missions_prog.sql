
  
  create view "duckdb_database"."main"."staging__tdb_ic_siicea_missions_prog__dbt_tmp" as (
    

WITH missions_prog AS (
    SELECT 
        "Identifiant de la mission" AS identifiant_mission,
        "Code thème IGAS" AS code_theme_igas,
        "Thème IGAS" AS theme_igas,
        "Type de mission" AS type_mission,
        "Modalité dinvestigation" AS modalite_d_investigation,
        "Type de planification" AS type_planification,
        "Modalité de la mission" AS modalite_mission,
        "Mission conjointe avec 1" AS mission_conjointe_1,
        "Mission conjointe avec 2" AS mission_conjointe_2,
        "Département" AS departement,
        "Commune" AS commune,
        "Adresse" AS adresse,
        "Groupe de cibles" AS groupe_cibles,
        "Cible" AS cible,
        "Caractère juridique" AS caractere_juridique,
        "Type de cible" AS type_cible,
        "Code FINESS" AS code_finess,
        "Statut de la mission" AS statut_mission,

        -- Formatage des dates
        CASE
            WHEN LENGTH("Date provisoire ""Visite""") = 10 
            THEN SUBSTRING("Date provisoire ""Visite""", 7, 4) || '-' ||
                 SUBSTRING("Date provisoire ""Visite""", 4, 2) || '-' ||
                 SUBSTRING("Date provisoire ""Visite""", 1, 2) || ' 00:00:00'
            ELSE "Date provisoire ""Visite"""
        END AS date_provisoire_visite,

        CASE
            WHEN LENGTH("Date réelle ""Visite""") = 10 
            THEN SUBSTRING("Date réelle ""Visite""", 7, 4) || '-' ||
                 SUBSTRING("Date réelle ""Visite""", 4, 2) || '-' ||
                 SUBSTRING("Date réelle ""Visite""", 1, 2) || ' 00:00:00'
            ELSE "Date réelle ""Visite"""
        END AS date_reelle_visite,

        "Date provisoire ""Fin Mission""" AS date_provisoire_fin_mission,
        "Date réelle ""Fin Mission""" AS date_reelle_fin_mission,

        -- Ajout du zéro devant les codes FINESS de longueur 8
        CASE 
            WHEN LENGTH("Code FINESS") = 8 THEN '0' || "Code FINESS"
            ELSE "Code FINESS"
        END AS cd_finess

    FROM "duckdb_database"."main"."staging__sa_siicea_missions"
    
    WHERE 1=1
    -- Filtrage sur la période
    AND (
        ("Date réelle ""Visite""" = '' 
            AND (CASE
                    WHEN LENGTH("Date provisoire ""Visite""") = 10 
                    THEN SUBSTRING("Date provisoire ""Visite""", 7, 4) || '-' ||
                         SUBSTRING("Date provisoire ""Visite""", 4, 2) || '-' ||
                         SUBSTRING("Date provisoire ""Visite""", 1, 2) || ' 00:00:00'
                    ELSE "Date provisoire ""Visite"""
                 END) > '2024-12-31 00:00:00') 
        OR (
            CASE
                WHEN LENGTH("Date réelle ""Visite""") = 10 
                THEN SUBSTRING("Date réelle ""Visite""", 7, 4) || '-' ||
                     SUBSTRING("Date réelle ""Visite""", 4, 2) || '-' ||
                     SUBSTRING("Date réelle ""Visite""", 1, 2) || ' 00:00:00'
                ELSE "Date réelle ""Visite"""
            END) > '2024-12-31 00:00:00'
    )
    
    -- Exclusion des statuts clôturés ou abandonnés
    AND "Statut de la mission" NOT IN ('Clôturé', 'Abandonné')
    
    --
    AND "Secteur dintervention" = 'Médico-social'
    
    -- Filtrage sur le type de cible
    AND "Type de cible" = 'Etablissements et Services pour Personnes Agées'
    
    -- Filtrage des thèmes IGAS pertinents
    AND "Code thème IGAS" IN (
        'MS634D13', 'MS634D13', 'MS634N1', 'MS634E1', 
        'MS634D12', 'MS634R1', 'MS634D11', 'MS634D15', 
        'MS634C10', 'MS634C10'
    )
    
    -- Exclusion des types de mission non pertinents
    AND "Type de mission" NOT IN (
        'Audit', 'Audit franco-wallon', 'Evaluation', 
        'Visites de conformité', 'Enquête administrative'
    )
)

SELECT * FROM missions_prog
  );
