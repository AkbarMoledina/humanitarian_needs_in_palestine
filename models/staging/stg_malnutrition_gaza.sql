WITH source AS (
    SELECT * FROM {{ source('raw', 'malnutrition_in_gaza')}}
),

cleaned AS (
    SELECT
        country, area, SAM, MAM, GAM
    FROM source
    WHERE SAM IS NOT NULL AND MAM IS NOT NULL AND GAM IS NOT NULL
)

SELECT * FROM cleaned