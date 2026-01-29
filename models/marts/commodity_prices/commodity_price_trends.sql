SELECT
    f.commodity_id,
    c.commodity_name,
    c.unit_amount,
    c.commodity_category,
    f.date_id,
    d.full_date,
    d.year AS price_year,
    d.month_name AS price_month,
    f.price
FROM {{ ref('fct_commodity_prices') }} f
LEFT JOIN {{ ref('dim_commodity') }} c
ON f.commodity_id = c.commodity_id
LEFT JOIN {{ ref('dim_date') }} d
ON f.date_id = d.date_id