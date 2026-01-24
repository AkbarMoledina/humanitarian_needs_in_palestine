WITH source AS (
    SELECT * FROM {{ source('raw', 'requirements_and_funding')}}
),

cleaned AS (
    SELECT
        id,
        name,
        code,
        typeId AS type_id,
        typeName AS type_name,
        startDate AS start_date,
        endDate AS end_date,
        year,
        requirements AS requirements_usd,
        funding AS funding_usd,
        percentFunded::DECIMAL AS funding_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num
        FROM source
    )
    WHERE row_num > 1
)

SELECT * FROM cleaned