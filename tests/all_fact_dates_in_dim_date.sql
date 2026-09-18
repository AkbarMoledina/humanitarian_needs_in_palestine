SELECT DISTINCT
    date_id
FROM {{ ref('fct_aid_received') }}
WHERE date_id NOT IN (SELECT date_id FROM {{ ref('dim_date') }})
