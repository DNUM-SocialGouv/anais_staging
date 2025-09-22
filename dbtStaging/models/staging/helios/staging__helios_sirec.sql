{{ config(
    materialized='view'
) }}

WITH sirec AS (
    SELECT * FROM {{ ref('staging__sa_sirec') }}
    WHERE date_de_reception_a_l_ars BETWEEN '{{ dbtStaging.get_first_day_of_x_years_ago(3) }}' AND '{{ dbtStaging.get_yesterday() }}'  -- A confirmer '2022-01-01' AND '2025-07-31'
)

SELECT * FROM sirec