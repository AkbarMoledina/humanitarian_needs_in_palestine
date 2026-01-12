SELECT
    commodity_name,
    unit_amount,
    price_date,
    price
FROM {{ ref('fct_commodity_prices_gaza') }}