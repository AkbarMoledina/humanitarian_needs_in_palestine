SELECT
    commodity_name,
    unit_amount,
    price_date,
    price
FROM {{ ref('stg_commodity_prices_gaza') }}

UNION ALL

SELECT DISTINCT
    commodity_name,
    unit_amount,
    '2023-10-01'::DATE AS price_date,
    avg_price_before_oct7 AS price

FROM {{ ref('stg_commodity_prices_gaza') }}
WHERE avg_price_before_oct7 IS NOT NULL
