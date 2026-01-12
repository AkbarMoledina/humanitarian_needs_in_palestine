SELECT
    country, area, SAM, MAM, GAM
FROM {{ ref('stg_malnutrition_gaza') }}
ORDER BY country, area