SELECT 
    f.aid_event_id
FROM {{ ref('fct_aid_received') }} f
LEFT JOIN {{ ref('dim_cargo') }} dc ON f.cargo_id = dc.cargo_id
WHERE f.cargo_id IS NOT NULL AND dc.cargo_id IS NULL
