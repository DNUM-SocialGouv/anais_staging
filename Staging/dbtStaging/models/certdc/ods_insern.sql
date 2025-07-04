{{ config(
    materialized='view'
) }}
{% set reference_date = get_reference_date('2025-03-01') %}

-- N-2
WITH N_2 AS (
    SELECT
        index,
        annee,
        mois,
        jour,
        departement_code,
        commune_code,
        et_finess,
        lieu_de_deces,
        source,
        deces_nb
    FROM {{ ref('staging__cert_dc_insern_n2_n1') }}
    WHERE 1 = 1
    -- param Ã  variabiliser dans le code
    AND annee::INT = {{ get_previous_year(3) }}
)
, N_1 AS (
    SELECT
        index,
        annee,
        mois,
        jour,
        departement_code,
        commune_code,
        et_finess,
        lieu_de_deces,
        source,
        deces_nb
    FROM {{ ref('staging__cert_dc_insern_2023_2024') }}
    WHERE 1 = 1
    -- param Ã  variabiliser dans le code
    AND annee::INT = {{ get_previous_year(2) }}
)
, N AS (
    SELECT
        index,
        annee,
        mois,
        jour,
        departement_code,
        commune_code,
        et_finess,
        lieu_de_deces,
        source,
        deces_nb
    FROM {{ ref('staging__cert_dc_insern_2023_2024') }}
    WHERE 1 = 1
    -- param Ã  variabiliser dans le code
    AND {{ where_remaining_last_year_months(reference_date=reference_date) }}
)
, N_0 AS (
    SELECT
        index,
        annee,
        mois,
        jour,
        departement_code,
        commune_code,
        et_finess,
        lieu_de_deces,
        source,
        deces_nb
    FROM {{ ref('staging__cert_dc_insern') }}
    WHERE 1 = 1
    -- param Ã  variabiliser dans le code
    AND {{ where_last_6_months(reference_date=reference_date) }}
)
, consolide AS (
    SELECT *
    FROM N_2
    UNION
    SELECT *
    FROM N_1
    UNION
    SELECT *
    FROM N
    UNION
    SELECT *
    FROM N_0
)


SELECT
    annee,
    mois,
    jour,
    CASE
        WHEN LENGTH(departement_code)=1 THEN '0' || departement_code
        ELSE departement_code
    END
    AS departement_code,
    CASE
        WHEN (LENGTH(departement_code) = 3 AND LENGTH(commune_code) = 1) THEN '0' || commune_code
        WHEN (LENGTH(departement_code) = 2 AND LENGTH(commune_code) = 1) THEN '00' || commune_code
        WHEN (LENGTH(departement_code) = 2 AND LENGTH(commune_code) = 2) THEN '0' || commune_code
        WHEN (LENGTH(departement_code) = 1 AND LENGTH(commune_code) = 1) THEN '00' || commune_code
        WHEN (LENGTH(departement_code) = 1 AND LENGTH(commune_code) = 2) THEN '0' || commune_code
        ELSE commune_code
    END
    AS commune_code,
    et_finess,
    lieu_de_deces,
    source,
    deces_nb
FROM consolide
