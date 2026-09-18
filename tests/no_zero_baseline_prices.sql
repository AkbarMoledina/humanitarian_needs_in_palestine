SELECT 
    commodity_id,
    date_id
FROM {{ ref('fct_commodity_prices') }}
WHERE date_id = 20231001 AND price = 0
