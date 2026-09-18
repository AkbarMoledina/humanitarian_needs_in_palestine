SELECT 
    commodity_id,
    date_id
FROM {{ ref('fct_commodity_prices') }}
WHERE date_id > CAST(REPLACE(CAST(CURRENT_DATE AS VARCHAR), '-', '') AS INT)
