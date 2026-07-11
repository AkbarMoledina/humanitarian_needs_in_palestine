WITH base AS (
    SELECT DISTINCT
        commodity_name,
        unit_amount
    FROM {{ ref('stg_commodity_prices') }}
),

category_mappings AS (
    SELECT * FROM {{ ref('commodity_categories') }}
),

-- Left join to find the first matching category for each commodity
categorized AS (
    SELECT DISTINCT
        b.commodity_name,
        b.unit_amount,
        COALESCE(
            MAX(CASE
                WHEN cm.match_type = 'like' AND b.commodity_name LIKE cm.commodity_pattern
                THEN cm.commodity_category
                WHEN cm.match_type = 'in' AND b.commodity_name = cm.commodity_pattern
                THEN cm.commodity_category
            END),
            'Other'
        ) AS commodity_category
    FROM base b
    CROSS JOIN category_mappings cm
    GROUP BY b.commodity_name, b.unit_amount
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'commodity_name',
        'unit_amount'
    ]) }} AS commodity_id,
    commodity_name,
    unit_amount,
    commodity_category
FROM categorized
