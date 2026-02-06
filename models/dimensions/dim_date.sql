WITH all_dates AS (
    SELECT DISTINCT price_date AS date
    FROM {{ ref('stg_commodity_prices') }}

    UNION

    SELECT DISTINCT received_date AS date
    FROM {{ ref('stg_aid_received') }}

    UNION

    SELECT DATE '2023-10-01' AS date
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
    DAYNAME(date) AS day_name
FROM all_dates