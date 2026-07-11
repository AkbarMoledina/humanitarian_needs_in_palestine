WITH date_range AS (
    SELECT
        MIN(min_date) AS start_date,
        MAX(max_date) AS end_date
    FROM (
        SELECT
            MIN(price_date) AS min_date,
            MAX(price_date) AS max_date
        FROM {{ ref('stg_commodity_prices') }}

        UNION ALL

        SELECT
            MIN(received_date) AS min_date,
            MAX(received_date) AS max_date
        FROM {{ ref('stg_aid_received') }}

        UNION ALL

        SELECT
            DATE '2023-10-01' AS min_date,
            CURRENT_DATE AS max_date

        ) AS dates
    ),


date_spine AS (
    SELECT UNNEST(
        GENERATE_SERIES(
            (SELECT start_date FROM date_range),
            (SELECT end_date FROM date_range),
            INTERVAL 1 DAY
        )
    ) AS date
)


SELECT
    STRFTIME(date, '%Y%m%d')::INT AS date_id,
    date AS full_date,
    YEAR(date) AS year,
    CASE
        WHEN MONTH(date) IN (1,2,3) THEN 'Q1'
        WHEN MONTH(date) IN (4,5,6) THEN 'Q2'
        WHEN MONTH(date) IN (7,8,9) THEN 'Q3'
        WHEN MONTH(date) IN (10,11,12) THEN 'Q4'
    END AS quarter,
    MONTH(date) AS month,
    MONTHNAME(date) AS month_name,
    WEEK(date) as week_of_year,
    DAY(date) AS day,
    DAYNAME(date) AS day_name,
    DAYOFWEEK(date) AS day_of_week,
    CASE WHEN DAYOFWEEK(date) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE
        WHEN full_date BETWEEN '2023-11-24' AND '2023-12-01' THEN 1
        WHEN full_date BETWEEN '2025-01-19' AND '2025-03-17' THEN 1
        WHEN full_date >= '2025-10-03' THEN 1
        ELSE 0 END AS is_ceasefire
FROM date_spine
ORDER BY date