{{ config(
    materialized='view'
) }}
{% set reference_date = '2025-07-31' %}

WITH sirec AS (
    SELECT * FROM {{ ref('staging__sa_sirec') }}
    WHERE date_de_reception_a_l_ars BETWEEN '{{ dbtStaging.get_first_day_of_x_years_ago(3, reference_date) }}' AND '{{ dbtStaging.get_yesterday(reference_date) }}'  -- A confirmer '2022-01-01' AND '2025-07-31'
)

SELECT * FROM sirec