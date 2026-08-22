{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['commodity_id', 'date_id']
) }}

SELECT
    d.commodity_id,
    STRFTIME(s.price_date, '%Y%m%d')::INT AS date_id,
    s.price,
    CURRENT_TIMESTAMP AS processed_date
FROM {{ ref('stg_commodity_prices') }} s
INNER JOIN {{ ref('dim_commodity') }} d
    ON s.commodity_name = d.commodity_name
    AND s.unit_amount = d.unit_amount

{% if is_incremental() %}
  WHERE STRFTIME(s.price_date, '%Y%m%d')::INT > (SELECT COALESCE(MAX(date_id), 0) FROM {{ this }})
{% endif %}

{% if not is_incremental() %}
UNION ALL

SELECT
    d.commodity_id,
    20231001 AS date_id,
    avg_price_before_oct7 AS price,
    CURRENT_TIMESTAMP AS processed_date
FROM {{ ref('stg_commodity_prices') }} s
INNER JOIN {{ ref('dim_commodity') }} d
    ON s.commodity_name = d.commodity_name
    AND s.unit_amount = d.unit_amount
WHERE avg_price_before_oct7 IS NOT NULL
{% endif %}
