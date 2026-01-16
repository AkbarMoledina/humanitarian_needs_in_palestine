
WITH baseline AS (
SELECT
    commodity_name,
    unit_amount,
    price_date,
    price AS baseline_price
FROM {{ ref('fct_commodity_prices_gaza') }}
WHERE price_date = '2023-10-01'
),

actual AS (
SELECT
    commodity_name,
    unit_amount,
    price_date,
    price
FROM {{ ref('fct_commodity_prices_gaza') }}
WHERE price_date > '2023-10-01'
)


SELECT
    a.commodity_name,
    a.unit_amount,
    a.price_date,
    a.price,
    b.baseline_price,
    ROUND((a.price - b.baseline_price / b.baseline_price) * 100, 2) AS pct_change_from_baseline
FROM actual a
INNER JOIN baseline b
ON a.commodity_name = b.commodity_name AND a.unit_amount = b.unit_amount