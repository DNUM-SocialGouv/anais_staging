

WITH cibles AS (
    SELECT 
        c.*,
        m."Date réelle ""Visite""" AS date_reelle_visite,
        m."Statut de la mission",
        m."Secteur dintervention",
        m."Type de mission"
    FROM "staging"."public"."staging__sa_siicea_cibles" c
    LEFT JOIN "staging"."public"."staging__sa_siicea_missions" m
        ON c.FINESS = m."Code FINESS"
),

filtered_cibles AS (
    SELECT *
    FROM cibles
    WHERE 
        -- Filtre sur la période (missions entre 2022 et 2024)
        date_reelle_visite BETWEEN '2022-01-01' AND '2024-12-31'
        -- Filtre sur les statuts de mission valides
        AND "Statut de la mission" IN ('Clôturé', 'Maintenu')
        -- Filtre sur le secteur d’intervention
        AND "Secteur dintervention" IN ('Médico-social', 'Sanitaire')
        -- Exclusion des types de mission non pertinents
        AND "Type de mission" NOT IN (
            'Audit', 'Audit franco-wallon', 
            'Evaluation', 'Visites de conformité', 
            'Enquête administrative'
        )
)

SELECT * FROM filtered_cibles