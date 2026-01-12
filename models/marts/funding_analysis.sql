SELECT
        id,
        name,
        code,
        type_id,
        type_name,
        start_date,
        end_date,
        year,
        requirements_usd,
        funding_usd,
        funding_pct,
    CASE
        WHEN funding_pct >= 100 THEN 'Fully Funded'
        WHEN funding_pct >= 50 THEN 'Partially Funded'
        ELSE 'Severely Underfunded'
    END AS funding_status
FROM {{ ref('stg_funding_pse') }}
ORDER BY year DESC, funding_pct ASC