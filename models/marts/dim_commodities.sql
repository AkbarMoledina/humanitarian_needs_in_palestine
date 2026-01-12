WITH base AS (
    SELECT DISTINCT
        commodity_name,
        unit_amount
    FROM {{ ref('stg_commodity_prices_gaza') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'commodity_name',
        'unit_amount'
    ]) }} AS commodity_id,
    commodity_name,
    unit_amount
FROM base
