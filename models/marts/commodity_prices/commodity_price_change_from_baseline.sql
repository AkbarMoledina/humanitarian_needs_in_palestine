WITH baseline AS (
    SELECT
        commodity_id,
        date_id,
        price AS baseline_price
    FROM {{ ref('fct_commodity_prices') }}
    WHERE date_id = 20231001
),

actual AS (
    SELECT
        commodity_id,
        date_id,
        price
    FROM {{ ref('fct_commodity_prices') }}
    WHERE date_id > 20231001
),

calc AS (
    SELECT
        a.commodity_id,
        a.date_id,
        a.price,
        b.baseline_price,
        ROUND(((a.price - b.baseline_price) / b.baseline_price) * 100, 2) AS pct_change_from_baseline
    FROM actual a
    INNER JOIN baseline b
    ON a.commodity_id = b.commodity_id
)

SELECT
    c.commodity_name,
    c.unit_amount,
    c.commodity_category,
    d.date,
    d.month_name,
    d.year,
    calc.price,
    calc.baseline_price,
    calc.pct_change_from_baseline
FROM calc
INNER JOIN {{ ref('dim_commodity') }} c
ON calc.commodity_id = c.commodity_id
INNER JOIN {{ ref('dim_date') }} d
ON calc.date_id = d.date_id