SELECT
    d.commodity_id,
    STRFTIME(s.price_date, '%Y%m%d')::INT AS date_id,
    s.price
FROM {{ ref('stg_commodity_prices') }} s
INNER JOIN {{ ref('dim_commodity') }} d
    ON s.commodity_name = d.commodity_name
    AND s.unit_amount = d.unit_amount

UNION ALL

SELECT DISTINCT
    d.commodity_id,
    20231001 AS date_id,
    avg_price_before_oct7 AS price
FROM {{ ref('stg_commodity_prices') }} s
INNER JOIN {{ ref('dim_commodity') }} d
    ON s.commodity_name = d.commodity_name
    AND s.unit_amount = d.unit_amount
WHERE avg_price_before_oct7 IS NOT NULL
