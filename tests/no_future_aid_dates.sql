SELECT 
    aid_event_id,
    date_id
FROM {{ ref('fct_aid_received') }}
WHERE date_id > CAST(REPLACE(CAST(CURRENT_DATE AS VARCHAR), '-', '') AS INT)
