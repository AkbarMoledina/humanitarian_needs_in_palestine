SELECT 
    f.aid_event_id
FROM {{ ref('fct_aid_received') }} f
LEFT JOIN {{ ref('dim_crossing') }} dc ON f.crossing_id = dc.crossing_id
WHERE f.crossing_id IS NOT NULL AND dc.crossing_id IS NULL
