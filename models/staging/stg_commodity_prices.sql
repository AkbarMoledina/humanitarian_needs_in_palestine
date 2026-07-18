WITH source AS (
    SELECT * FROM {{ source('raw', 'commodity_prices') }}
),

cleaned AS (
    SELECT
        LOWER(TRIM("commodity name (english)")) AS commodity_name_raw,
        LOWER(TRANSLATE("amount (english)", '()', '')) AS unit_amount_raw,
        price_date::DATE AS price_date,
        price::DECIMAL AS price,
        ROUND("average price before 7 October 2023", 2) AS avg_price_before_oct7,
        CURRENT_TIMESTAMP AS loaded_at
    FROM source
    WHERE price IS NOT NULL
),

enriched AS (
    SELECT
        commodity_name_raw,
        --Removes unit amount from commodity_name
        CASE
            WHEN REGEXP_MATCHES(commodity_name_raw, '\([0-9].*?\)')
            THEN TRIM(REGEXP_REPLACE(commodity_name_raw, '\([0-9].*?\)', ''))
            ELSE commodity_name_raw
        END AS commodity_name,
        unit_amount_raw,
        --Looks for and moves unit amount from commodity_name_raw to unit_amount if unit_amount is null
        CASE
            WHEN unit_amount_raw IS NULL
             AND REGEXP_MATCHES(commodity_name_raw, '\([0-9].*?\)')
            THEN REGEXP_REPLACE(REGEXP_EXTRACT(commodity_name_raw, '\([0-9][^)]*\)'), '[()]', '')
            ELSE unit_amount_raw
        END AS unit_amount,
        price_date,
        price,
        avg_price_before_oct7
    FROM cleaned
),

standardised_units AS (
SELECT
    commodity_name,
    REGEXP_REPLACE(unit_amount, 'liters|litres|liter|litre', 'L', 'gi') AS unit_amount,
    price_date,
    price,
    avg_price_before_oct7
FROM enriched
)

SELECT
    commodity_name,
    unit_amount,
    price_date,
    price,
    avg_price_before_oct7
FROM standardised_units
