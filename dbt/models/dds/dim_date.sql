{{ config(
    materialized='table',
    distributed_by='date'
) }}

WITH date_bounds AS (
    SELECT
        MIN(orderdate::date) AS min_date,
        MAX(orderdate::date) AS max_date
    FROM {{ ref('fact_sales') }}
),

calendar AS (
    SELECT
        generate_series(
            min_date,
            max_date,
            interval '1 day'
        )::date AS date
    FROM date_bounds
)

SELECT
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(QUARTER FROM date) AS quarter,
    NOW() AS _dds_load_dttm
FROM calendar