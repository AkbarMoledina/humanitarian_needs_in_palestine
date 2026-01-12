WITH source AS (
    SELECT * FROM {{ source('raw', 'commodity_prices_gaza') }}
),

cleaned AS (

    SELECT
        LOWER(TRIM("commodity name (english)")) AS commodity_name_raw,
        LOWER(TRANSLATE("amount (english)", '()', '')) AS unit_amount_raw,
        price_date::DATE AS price_date,
        price::DECIMAL AS price,
        ROUND("average price before 7 October 2023", 2) AS avg_price_before_oct7,
        -- Add metadata
        current_timestamp AS loaded_at

    FROM source
    WHERE price IS NOT NULL
),

enriched AS (
    SELECT
        commodity_name_raw,
        --Removes unit amount from commodity_name
        CASE
            WHEN regexp_matches(commodity_name_raw, '\([0-9].*?\)')
            THEN TRIM(regexp_replace(commodity_name_raw, '\([0-9].*?\)', ''))
            ELSE commodity_name_raw
        END AS commodity_name,
        unit_amount_raw,
        --Looks for and moves unit amount from commodity_name_raw to unit_amount if unit_amount is null
        CASE
            WHEN unit_amount_raw IS NULL
             AND regexp_matches(commodity_name_raw, '\([0-9].*?\)')
            THEN regexp_replace(regexp_extract(commodity_name_raw, '\([0-9][^)]*\)'), '[()]', '')
            ELSE unit_amount_raw
        END AS unit_amount,
        price_date,
        price,
        avg_price_before_oct7
    FROM cleaned
)

SELECT
    commodity_name,
    unit_amount,
    price_date,
    price,
    avg_price_before_oct7
FROM enriched
