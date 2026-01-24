SELECT
    commodity_id,
    date_id,
    price
FROM {{ ref('fct_commodity_prices') }}